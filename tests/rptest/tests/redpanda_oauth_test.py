# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.tests.test import Test
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool, RpkException
from rptest.clients.python_librdkafka import PythonLibrdkafka
from rptest.clients.kafka_cli_tools import KafkaCliTools, AuthorizationError
from rptest.services.redpanda import LoggingConfig, MetricsEndpoint, PandaproxyConfig, SchemaRegistryConfig, SecurityConfig, make_redpanda_service
from rptest.services.keycloak import DEFAULT_REALM, DEFAULT_AT_LIFESPAN_S, KeycloakService
from rptest.services.cluster import cluster
from rptest.tests.sasl_reauth_test import get_sasl_metrics, REAUTH_METRIC, EXPIRATION_METRIC
from rptest.util import expect_exception

import requests
import time
from keycloak import KeycloakOpenID
from urllib.parse import urlparse
import json

CLIENT_ID = 'myapp'
TOKEN_AUDIENCE = 'account'
EXAMPLE_TOPIC = 'foo'

log_config = LoggingConfig('info',
                           logger_levels={
                               'security': 'trace',
                               'pandaproxy': 'trace',
                               'kafka/client': 'trace',
                               'kafka': 'debug',
                               'http': 'trace',
                           })


class RedpandaOIDCTestBase(Test):
    """
    Base class for tests that use the Redpanda service with OIDC
    """
    def __init__(self,
                 test_context,
                 num_nodes=4,
                 sasl_mechanisms=['SCRAM', 'OAUTHBEARER'],
                 http_authentication=["BASIC", "OIDC"],
                 sasl_max_reauth_ms=None,
                 access_token_lifespan=DEFAULT_AT_LIFESPAN_S,
                 **kwargs):
        super(RedpandaOIDCTestBase, self).__init__(test_context, **kwargs)
        self.produce_messages = []
        self.produce_errors = []
        num_brokers = num_nodes - 1
        self.keycloak = KeycloakService(test_context)
        kc_node = self.keycloak.nodes[0]
        try:
            self.keycloak.start_node(
                kc_node, access_token_lifespan_s=access_token_lifespan)
        except Exception as e:
            self.logger.error(f"{e}")
            self.keycloak.clean_node(kc_node)
            assert False, "Keycloak failed to start"

        security = SecurityConfig()
        security.enable_sasl = True
        security.sasl_mechanisms = sasl_mechanisms
        security.http_authentication = http_authentication

        pandaproxy_config = PandaproxyConfig()
        pandaproxy_config.authn_method = 'http_basic'

        schema_reg_config = SchemaRegistryConfig()
        schema_reg_config.authn_method = 'http_basic'

        self.redpanda = make_redpanda_service(
            test_context,
            num_brokers,
            extra_rp_conf={
                "oidc_discovery_url": self.keycloak.get_discovery_url(kc_node),
                "oidc_token_audience": TOKEN_AUDIENCE,
                "kafka_sasl_max_reauth_ms": sasl_max_reauth_ms,
            },
            security=security,
            pandaproxy_config=pandaproxy_config,
            schema_registry_config=schema_reg_config,
            log_config=log_config)

        self.su_username, self.su_password, self.su_algorithm = self.redpanda.SUPERUSER_CREDENTIALS

        self.rpk = RpkTool(self.redpanda,
                           username=self.su_username,
                           password=self.su_password,
                           sasl_mechanism=self.su_algorithm)

    def setUp(self):
        self.produce_messages.clear()
        self.produce_errors.clear()
        self.redpanda.logger.info("Starting Redpanda")
        self.redpanda.start()

    def create_service_user(self, client_id=CLIENT_ID):
        kc_node = self.keycloak.nodes[0]

        self.keycloak.admin.create_user('norma',
                                        'desmond',
                                        realm_admin=True,
                                        email='10086@sunset.blvd')
        self.keycloak.login_admin_user(kc_node, 'norma', 'desmond')
        self.keycloak.admin.create_client(client_id)

        service_user = f'service-account-{client_id}'
        # add an email address to myapp client's service user. this should
        # appear alongside the access token.
        self.keycloak.admin.update_user(service_user,
                                        email='myapp@customer.com')
        return self.keycloak.admin_ll.get_user_id(service_user)

    def get_client_credentials_token(self, cfg):
        token_endpoint_url = urlparse(cfg.token_endpoint)
        openid = KeycloakOpenID(
            server_url=
            f'{token_endpoint_url.scheme}://{token_endpoint_url.netloc}',
            client_id=cfg.client_id,
            client_secret_key=cfg.client_secret,
            realm_name=DEFAULT_REALM,
            verify=True)
        return openid.token(grant_type="client_credentials")


class RedpandaOIDCTest(RedpandaOIDCTestBase):
    @cluster(num_nodes=4)
    def test_init(self):
        kc_node = self.keycloak.nodes[0]

        client_id = CLIENT_ID
        service_user_id = self.create_service_user()

        self.rpk.create_topic(EXAMPLE_TOPIC)
        result = self.rpk.sasl_allow_principal(f'User:{service_user_id}',
                                               ['all'], 'topic', EXAMPLE_TOPIC,
                                               self.su_username,
                                               self.su_password,
                                               self.su_algorithm)

        cfg = self.keycloak.generate_oauth_config(kc_node, client_id)
        assert cfg.client_secret is not None
        assert cfg.token_endpoint is not None
        k_client = PythonLibrdkafka(self.redpanda,
                                    algorithm='OAUTHBEARER',
                                    oauth_config=cfg)
        producer = k_client.get_producer()

        # Explicit poll triggers OIDC token flow. Required for librdkafka
        # metadata requests to behave nicely.
        producer.poll(0.0)

        expected_topics = set([EXAMPLE_TOPIC])
        print(f'expected_topics: {expected_topics}')

        wait_until(lambda: set(producer.list_topics(timeout=5).topics.keys())
                   == expected_topics,
                   timeout_sec=5)

        token = self.get_client_credentials_token(cfg)

        def check_pp_topics():
            response = requests.get(
                url=
                f'http://{self.redpanda.nodes[0].account.hostname}:8082/topics',
                headers={
                    'Accept': 'application/vnd.kafka.v2+json',
                    'Content-Type': 'application/vnd.kafka.v2+json',
                    'Authorization': f'Bearer {token["access_token"]}'
                },
                timeout=5)
            return response.status_code == requests.codes.ok and set(
                response.json()) == expected_topics

        def check_sr_subjects():
            response = requests.get(
                url=
                f'http://{self.redpanda.nodes[0].account.hostname}:8081/subjects',
                headers={
                    'Accept': 'application/vnd.schemaregistry.v1+json',
                    'Authorization': f'Bearer {token["access_token"]}'
                },
                timeout=5)
            return response.status_code == requests.codes.ok and response.json(
            ) == []

        wait_until(check_pp_topics, timeout_sec=5)

        wait_until(check_sr_subjects, timeout_sec=5)

    @cluster(num_nodes=4)
    def test_java_client(self):
        kc_node = self.keycloak.nodes[0]

        client_id = CLIENT_ID
        service_user_id = self.create_service_user(client_id)

        self.rpk.create_topic(EXAMPLE_TOPIC)
        expected_topics = set([EXAMPLE_TOPIC])
        wait_until(lambda: set(self.rpk.list_topics()) == expected_topics,
                   timeout_sec=5)

        cfg = self.keycloak.generate_oauth_config(kc_node, client_id)
        cli = KafkaCliTools(self.redpanda, oauth_cfg=cfg)

        self.redpanda.logger.debug(
            "Without an appropriate ACL, topic list empty, produce fails with an authZ error."
        )

        with expect_exception(AuthorizationError, lambda _: True):
            cli.oauth_produce(EXAMPLE_TOPIC, 1)

        assert len(cli.list_topics()) == 0

        self.redpanda.logger.debug(
            "Grant access to service user. We can see it in the list and produce now."
        )

        self.rpk.sasl_allow_principal(f'User:{service_user_id}', ['all'],
                                      'topic', EXAMPLE_TOPIC, self.su_username,
                                      self.su_password, self.su_algorithm)

        assert set(cli.list_topics()) == set(expected_topics)

        N_REC = 10
        cli.oauth_produce(EXAMPLE_TOPIC, N_REC)
        records = []
        for i in range(0, N_REC):
            rec = self.rpk.consume(EXAMPLE_TOPIC, n=1, offset=i)
            records.append(json.loads(rec))

        self.redpanda.logger.debug(json.dumps(records, indent=1))

        assert len(
            records) == N_REC, f"Expected {N_REC} records, got {len(records)}"

        values = set([r['value'] for r in records])

        assert len(values) == len(
            records
        ), f"Expected {len(records)} unique records, got {len(values)}"

    @cluster(num_nodes=4)
    def test_admin_whoami(self):
        kc_node = self.keycloak.nodes[0]
        rp_node = self.redpanda.nodes[0]

        client_id = CLIENT_ID
        service_user_id = self.create_service_user(client_id)
        cfg = self.keycloak.generate_oauth_config(kc_node, client_id)
        token = self.get_client_credentials_token(cfg)

        whoami_url = f'http://{self.redpanda.admin_endpoint(rp_node)}/v1/security/oidc/whoami'
        auth_header = {'Authorization': f'Bearer {token["access_token"]}'}

        def request_whoami(with_auth: bool):
            response = requests.get(url=whoami_url,
                                    headers=auth_header if with_auth else None,
                                    timeout=5)
            self.redpanda.logger.info(
                f'response.status_code: {response.status_code}, response.content: {response.content}'
            )
            return response

        # At this point, admin API does not require auth and service_user_id is not a superuser

        response = request_whoami(with_auth=False)
        assert response.status_code == requests.codes.unauthorized

        response = request_whoami(with_auth=True)
        assert response.status_code == requests.codes.ok
        assert response.json()['id'] == service_user_id
        assert response.json()['expire'] > time.time()

        # Require Auth for Admin
        self.redpanda.set_cluster_config({'admin_api_require_auth': True})

        response = request_whoami(with_auth=False)
        assert response.status_code == requests.codes.unauthorized

        response = request_whoami(with_auth=True)
        assert response.status_code == requests.codes.ok
        assert response.json()['id'] == service_user_id
        assert response.json()['expire'] > time.time()

    @cluster(num_nodes=4)
    def test_admin_invalidate_keys(self):
        kc_node = self.keycloak.nodes[0]
        rp_node = self.redpanda.nodes[0]

        def get_idp_request_count():
            metrics = [
                "security_idp_latency_seconds_count",
            ]
            samples = self.redpanda.metrics_samples(metrics, [rp_node],
                                                    MetricsEndpoint.METRICS)

            result = {}
            for k in samples.keys():
                result[k] = result.get(k, 0) + sum(
                    [int(s.value) for s in samples[k].samples])
            return result["security_idp_latency_seconds_count"]

        client_id = CLIENT_ID
        service_user_id = self.create_service_user(client_id)
        cfg = self.keycloak.generate_oauth_config(kc_node, client_id)
        token = self.get_client_credentials_token(cfg)

        invalidate_keys_url = f'http://{self.redpanda.admin_endpoint(rp_node)}/v1/security/oidc/keys/cache_invalidate'
        auth_header = {'Authorization': f'Bearer {token["access_token"]}'}

        def request_cache_invalidate(with_auth: bool):
            response = requests.post(
                url=invalidate_keys_url,
                headers=auth_header if with_auth else None,
                timeout=5)
            self.redpanda.logger.info(
                f'response.status_code: {response.status_code}')
            return response.status_code

        # At this point, admin API does not require auth and service_user_id is not a superuser

        assert request_cache_invalidate(with_auth=False) == requests.codes.ok
        assert request_cache_invalidate(with_auth=True) == requests.codes.ok

        # Require Auth for Admin
        self.redpanda.set_cluster_config({'admin_api_require_auth': True})

        assert request_cache_invalidate(
            with_auth=False) == requests.codes.forbidden
        assert request_cache_invalidate(
            with_auth=True) == requests.codes.forbidden

        # Add service_user_id as a superuser
        self.redpanda.set_cluster_config({
            'superusers':
            [self.redpanda.SUPERUSER_CREDENTIALS.username, service_user_id]
        })

        id_requests = get_idp_request_count()

        assert request_cache_invalidate(with_auth=True) == requests.codes.ok

        assert id_requests < get_idp_request_count()


class OIDCReauthTest(RedpandaOIDCTestBase):
    MAX_REAUTH_MS = 8000
    PRODUCE_DURATION_S = MAX_REAUTH_MS * 2 / 1000
    PRODUCE_INTERVAL_S = 0.1
    PRODUCE_ITER = int(PRODUCE_DURATION_S / PRODUCE_INTERVAL_S)

    TOKEN_LIFESPAN_S = int(PRODUCE_DURATION_S)

    def __init__(self, test_context, **kwargs):
        super().__init__(test_context,
                         sasl_max_reauth_ms=self.MAX_REAUTH_MS,
                         access_token_lifespan=self.TOKEN_LIFESPAN_S,
                         **kwargs)

    @cluster(num_nodes=4)
    def test_oidc_reauth(self):
        kc_node = self.keycloak.nodes[0]

        client_id = CLIENT_ID
        service_user_id = self.create_service_user(client_id)

        self.rpk.create_topic(EXAMPLE_TOPIC)
        self.rpk.sasl_allow_principal(f'User:{service_user_id}', ['all'],
                                      'topic', EXAMPLE_TOPIC, self.su_username,
                                      self.su_password, self.su_algorithm)

        cfg = self.keycloak.generate_oauth_config(kc_node, CLIENT_ID)
        assert cfg.client_secret is not None
        assert cfg.token_endpoint is not None
        k_client = PythonLibrdkafka(self.redpanda,
                                    algorithm='OAUTHBEARER',
                                    oauth_config=cfg)
        producer = k_client.get_producer()
        producer.poll(1.0)

        expected_topics = set([EXAMPLE_TOPIC])
        wait_until(lambda: set(producer.list_topics(timeout=5).topics.keys())
                   == expected_topics,
                   timeout_sec=5)

        for _ in range(0, self.PRODUCE_ITER):
            producer.poll(0.0)
            producer.produce(topic=EXAMPLE_TOPIC, key='bar', value='23')
            time.sleep(self.PRODUCE_INTERVAL_S)

        producer.flush(timeout=5)

        metrics = get_sasl_metrics(self.redpanda)
        self.redpanda.logger.debug(f"SASL metrics: {metrics}")
        assert (EXPIRATION_METRIC in metrics.keys())
        assert (metrics[EXPIRATION_METRIC] == 0
                ), "Client should reauth before session expiry"
        assert (REAUTH_METRIC in metrics.keys())
        assert (metrics[REAUTH_METRIC]
                > 0), "Expected client reauth on some broker..."

        assert k_client.oauth_count == 2, f"Expected 2 OAUTH challenges, got {k_client.oauth_count}"
