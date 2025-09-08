PROJECT = rabbitmq_stream_s3
PROJECT_DESCRIPTION = RabbitMQ S3 plugin
PROJECT_MOD = rabbitmq_stream_s3_app

DEPS = rabbit_common rabbit osiris
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

include ../../rabbitmq-components.mk
include ../../erlang.mk
