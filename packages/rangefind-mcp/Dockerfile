# Container for the rangefind MCP server (used by registry sandboxes such as
# Glama for introspection, and usable standalone). Runs in open mode by
# default — tools accept any index path or URL; pass --index name=url args
# to restrict to named indexes.
FROM node:22-slim
RUN npm install -g rangefind-mcp@0.3.2
ENTRYPOINT ["rangefind-mcp"]
