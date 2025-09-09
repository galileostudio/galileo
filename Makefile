.PHONY: setup test lint install

setup:
	@echo "Setting up Galileo development environment..."
	cd galileo-analyzers && pip install -r requirements.txt
	cd web && bundle install
	cd web && rails db:create db:migrate

test:
	cd galileo-analyzers && python -m pytest tests/
	cd web && rspec

lint:
	cd galileo-analyzers && pylint galileo/
	cd web && rubocop

install:
	cd galileo-analyzers && pip install -e .

dev:
	docker-compose up

clean:
	docker-compose down -v
	docker system prune -f