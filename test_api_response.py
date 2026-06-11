#!/usr/bin/env python3
"""临时调试脚本:检查 /api/nodes 返回的数据结构"""
import json
from web_app import app
from fastapi.testclient import TestClient

client = TestClient(app)

# 模拟登录
response = client.post("/api/auth/login", json={"username": "admin", "password": "test123"})
print(f"Login status: {response.status_code}")

# 获取节点数据
response = client.get("/api/nodes?hours=24")
print(f"\n/api/nodes status: {response.status_code}\n")

if response.status_code == 200:
    data = response.json()
    nodes = data.get("data", {}).get("nodes", [])
    if nodes:
        first_node = nodes[0]
        print("First node keys:", list(first_node.keys()))
        print("\nFirst node structure:")
        print(json.dumps(first_node, indent=2, ensure_ascii=False))

        # 检查健康数据
        print(f"\nHealth data in first node:")
        print(f"  cpu: {first_node.get('cpu')}")
        print(f"  ram: {first_node.get('ram')}")
        print(f"  disk: {first_node.get('disk')}")

        # 检查komari嵌套
        if 'komari' in first_node:
            machine = first_node['komari'].get('machine')
            print(f"\nKomari machine data:")
            if machine:
                print(f"  machine.cpu: {machine.get('cpu')}")
                print(f"  machine.ram: {machine.get('ram')}")
                print(f"  machine.disk: {machine.get('disk')}")
            else:
                print(f"  machine is None/empty")
    else:
        print("No nodes in response")
else:
    print(f"Error: {response.text}")
