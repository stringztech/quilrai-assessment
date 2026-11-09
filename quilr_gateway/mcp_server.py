"""A stdio MCP server with typed validation and simulated refunds."""

import asyncio
import json
import logging
import sys
from typing import Annotated

from mcp import types
from mcp.server.lowlevel import Server
from mcp.server.stdio import stdio_server
from mcp.shared.exceptions import McpError
from pydantic import BaseModel, ConfigDict, Field, StringConstraints, ValidationError

logger = logging.getLogger(__name__)
server = Server("customer-service")
CustomerId = Annotated[str, StringConstraints(pattern=r"^CUST-[0-9]{5}$")]
RefundReason = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=10)
]


class CustomerRequest(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    customer_id: CustomerId


class RefundRequest(CustomerRequest):
    amount: Annotated[float, Field(gt=0, allow_inf_nan=False)]
    reason: RefundReason


TOOLS = {
    "get_customer_record": (CustomerRequest, "Look up a synthetic customer."),
    "trigger_refund": (RefundRequest, "Simulate a refund. No payment is made."),
}

CUSTOMERS = {
    "CUST-00001": {"name": "Demo Customer", "status": "active"},
    "CUST-12345": {"name": "Sample Customer", "status": "active"},
}


@server.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(name=name, description=description, inputSchema=schema.model_json_schema())
        for name, (schema, description) in TOOLS.items()
    ]


async def call_tool(request: types.CallToolRequest) -> types.ServerResult:
    name = request.params.name
    if name not in TOOLS:
        raise McpError(types.ErrorData(code=types.INVALID_PARAMS, message="Unknown tool"))
    schema, _ = TOOLS[name]
    try:
        validated = schema.model_validate(request.params.arguments or {})
    except ValidationError as error:
        fields = [
            {"field": ".".join(map(str, issue["loc"])), "type": issue["type"]}
            for issue in error.errors(include_input=False, include_context=False, include_url=False)
        ]
        raise McpError(
            types.ErrorData(code=types.INVALID_PARAMS, message="Invalid tool arguments", data=fields)
        ) from None

    customer = CUSTOMERS.get(validated.customer_id)
    if customer is None:
        return types.ServerResult(
            types.CallToolResult(
                isError=True,
                content=[types.TextContent(type="text", text="Customer not found")],
            )
        )

    if isinstance(validated, RefundRequest):
        result = {"customer_id": validated.customer_id, "amount": validated.amount, "status": "simulated"}
    else:
        result = {"customer_id": validated.customer_id, **customer}

    return types.ServerResult(
        types.CallToolResult(
            content=[types.TextContent(type="text", text=json.dumps(result))],
            structuredContent=result,
        )
    )


# The SDK decorator converts handler exceptions to tool results. The SDK's
# typed handler registry lets validation failures surface as JSON-RPC errors.
server.request_handlers[types.CallToolRequest] = call_tool


async def main() -> None:
    logging.basicConfig(level=logging.INFO, stream=sys.stderr)
    logger.info("Starting customer MCP server over stdio")
    async with stdio_server() as (reader, writer):
        await server.run(reader, writer, server.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
