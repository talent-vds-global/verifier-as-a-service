"""Kiểm MCP server đầu-cuối bằng MCP client thật qua stdio — đúng cách Claude Code nối vào.

Đi đủ vòng của generator: list_contracts → request_verification → get_verification_result →
get_evidence cho tiêu chí trượt đầu tiên. Cần app verifier đang chạy (VERIFIER_URL, mặc định
http://localhost:8100) và stack ewallet-demo.

    python test_mcp.py                 # chạy đủ 3 nhánh × trial theo hợp đồng
    python test_mcp.py --quick         # chỉ F1.held × 1 trial
"""

import re
import sys

import anyio
from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client


def text_of(result) -> str:
    return "\n".join(getattr(c, "text", "") for c in result.content)


async def main(quick: bool) -> int:
    params = StdioServerParameters(command=sys.executable, args=["mcp_server.py"])
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            init = await session.initialize()
            print(f"server: {init.server_info.name} {init.server_info.version}")
            tools = [t.name for t in (await session.list_tools()).tools]
            print(f"tools: {tools}")
            assert set(tools) == {"list_contracts", "request_verification",
                                  "get_verification_result", "get_evidence"}, tools

            contracts = text_of(await session.call_tool("list_contracts", {}))
            print("\n--- list_contracts ---\n" + "\n".join(contracts.splitlines()[:6]) + "\n...")
            sha = re.search(r"contract_sha256: ([0-9a-f]{64})", contracts).group(1)

            args = {"contract_id": "F1", "expected_sha256": sha, "note": "test_mcp"}
            if quick:
                args.update(scenarios=["F1.held"], trials=1)
            req = text_of(await session.call_tool("request_verification", args))
            print("\n--- request_verification ---\n" + req)
            vid = re.search(r"verification_id: (\S+)", req).group(1)

            res = text_of(await session.call_tool("get_verification_result", {"verification_id": vid}))
            print("\n--- get_verification_result ---\n" + res)

            failed = re.findall(r"\| `([^`]+)`", res)
            if failed:
                ev = text_of(await session.call_tool("get_evidence",
                                                     {"verification_id": vid, "criterion": failed[0]}))
                print(f"\n--- get_evidence {failed[0]} ---\n" + ev[:1500])
            bad = text_of(await session.call_tool("get_evidence",
                                                  {"verification_id": vid, "criterion": "F1.KHONG-CO"}))
            print("\n--- get_evidence tiêu chí không tồn tại ---\n" + bad[:200])
    return 0


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    sys.exit(anyio.run(main, "--quick" in sys.argv))
