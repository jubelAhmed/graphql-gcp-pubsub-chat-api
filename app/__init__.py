from ariadne import make_executable_schema, load_schema_from_path, \
    snake_case_fallback_resolvers
from ariadne.asgi import GraphQL

from app.mutations import mutation
from app.queries import query
from app.subscriptions import subscription



type_defs = load_schema_from_path("schema.graphql")

schema = make_executable_schema(type_defs, query, mutation, subscription, snake_case_fallback_resolvers)

app = GraphQL(schema, debug=True)

