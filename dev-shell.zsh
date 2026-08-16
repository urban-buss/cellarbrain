# Source this file from zsh: source ./dev-shell.zsh
export APP_ENV=development
export CELLARBRAIN_CONFIG="$PWD/cellarbrain.local.toml"
export CELLARBRAIN_DATA_DIR="$PWD/output_dev"
export DUCKDB_PATH="$PWD/data/dev/cellarbrain.duckdb"
export DATABASE_URL="postgresql://postgres:devpass@127.0.0.1:5433/cellarbrain_dev"
export API_HOST=127.0.0.1
export API_PORT=8081

if [[ -f .venv/bin/activate ]]; then
  source .venv/bin/activate
fi

print "cellarbrain development environment active"
print "  config: $CELLARBRAIN_CONFIG"
print "  data:   $CELLARBRAIN_DATA_DIR"
print "  port:   $API_HOST:$API_PORT"
