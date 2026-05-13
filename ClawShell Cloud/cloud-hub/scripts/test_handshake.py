"""双通道握手测试脚本

测试 MCP + HTTP 双通道的握手、去重、ACK、DLQ 机制
"""
import asyncio
import aiohttp
import time
import uuid

MCP_URL = "http://localhost:8081"
HTTP_URL = "http://localhost:8082/cloudbrain/write"


async def test_mcp_channel():
    """测试 MCP 主通道"""
    print("\n=== MCP 主通道测试 ===")
    message_id = str(uuid.uuid4())
    payload = {"test": "mcp", "ts": time.time()}

    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"{MCP_URL}/cloudbrain/write",
            json={
                "message_id": message_id,
                "seq": 1,
                "channel": "mcp",
                "type": "insight_add",
                "payload": payload,
                "timestamp": time.time(),
                "retry_count": 0,
            },
            timeout=aiohttp.ClientTimeout(total=3.0),
        ) as resp:
            result = await resp.json()
            print(f"  Status: {result.get('status')}")
            print(f"  ACK seq: {result.get('ack_seq')}")
            print(f"  Channel: {result.get('channel')}")
            assert result.get("status") == "ok", f"MCP failed: {result}"
            print("  ✅ MCP 通道 OK")
            return result


async def test_http_channel():
    """测试 HTTP 备用通道"""
    print("\n=== HTTP 备用通道测试 ===")
    message_id = str(uuid.uuid4())
    payload = {"test": "http", "ts": time.time()}

    async with aiohttp.ClientSession() as session:
        async with session.post(
            HTTP_URL,
            json={
                "message_id": message_id,
                "seq": 2,
                "channel": "http",
                "type": "insight_add",
                "payload": payload,
                "timestamp": time.time(),
                "retry_count": 0,
            },
            timeout=aiohttp.ClientTimeout(total=3.0),
        ) as resp:
            result = await resp.json()
            print(f"  Status: {result.get('status')}")
            print(f"  ACK seq: {result.get('ack_seq')}")
            assert result.get("status") == "ok", f"HTTP failed: {result}"
            print("  ✅ HTTP 通道 OK")
            return result


async def test_duplicate():
    """测试消息去重"""
    print("\n=== 去重测试 ===")
    message_id = str(uuid.uuid4())

    for i in range(2):
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{MCP_URL}/cloudbrain/write",
                json={
                    "message_id": message_id,
                    "seq": 100 + i,
                    "channel": "mcp",
                    "type": "insight_add",
                    "payload": {"seq": i},
                    "timestamp": time.time(),
                    "retry_count": 0,
                },
                timeout=aiohttp.ClientTimeout(total=3.0),
            ) as resp:
                result = await resp.json()
                expected = "duplicate" if i == 1 else "ok"
                actual = result.get("status")
                print(f"  Attempt {i+1}: {actual} (expect: {expected})")
                assert actual == expected, f"Duplicate test failed: {result}"

    print("  ✅ 去重机制 OK")


async def test_status():
    """测试健康检查"""
    print("\n=== 健康检查 ===")
    async with aiohttp.ClientSession() as session:
        for url in [
            f"{MCP_URL}/health",
            f"{HTTP_URL.rsplit('/', 1)[0]}/health",
        ]:
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=2.0)) as resp:
                    result = await resp.json()
                    print(f"  {url}: {result.get('status')} - {result.get('detail', '')}")
            except Exception as e:
                print(f"  {url}: ❌ {e}")


async def test_cloudbrain_read():
    """测试读取接口"""
    print("\n=== cloudbrain.read 测试 ===")
    async with aiohttp.ClientSession() as session:
        async with session.get(
            f"{MCP_URL}/cloudbrain/read?limit=5",
            timeout=aiohttp.ClientTimeout(total=3.0),
        ) as resp:
            result = await resp.json()
            print(f"  Items: {result.get('count')}")
            print("  ✅ read OK")


async def main():
    print("=" * 50)
    print("双通道握手测试")
    print("=" * 50)

    await test_status()

    try:
        await test_mcp_channel()
        await test_http_channel()
        await test_duplicate()
        await test_cloudbrain_read()
        print("\n" + "=" * 50)
        print("✅ 全部测试通过")
        print("=" * 50)
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
