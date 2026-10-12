.PHONY: help init start stop status restart test clean

help:
	@echo "Kafka Management Commands:"
	@echo "  make init      - Format KRaft storage (only if uninitialized)"
	@echo "  make start     - Start Kafka broker locally (512MB heap)"
	@echo "  make stop      - Stop running Kafka broker"
	@echo "  make status    - Check Kafka broker status and port 9092"
	@echo "  make restart   - Restart Kafka broker"
	@echo "  make test      - Run produce & consume smoke test"
	@echo "  make clean     - Stop broker and clear local data/logs"

init:
	@./scripts/init-kraft.sh

start:
	@./scripts/start-kafka.sh

stop:
	@./scripts/stop-kafka.sh

restart: stop start

status:
	@./scripts/status-kafka.sh

test:
	@./scripts/smoke-test.sh

clean: stop
	@echo "Clearing Kafka data and logs..."
	@rm -rf data/ logs/ .kafka.pid .cluster_id
	@echo "Kafka clean complete."
