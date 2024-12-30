set -eo pipefail
mkdir -p lib/nodejs
rm -rf node_modules lib/nodejs/node_modules lib/nodejs/*
npm install --silent --production
mv node_modules lib/nodejs/
npm install --silent