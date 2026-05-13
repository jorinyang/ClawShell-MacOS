#!/bin/bash
# 阿里云安全组配置脚本
# 用法：bash setup_security_group.sh <your-ecs-ip> <region>

set -e

ECS_IP="${1:?用法: bash setup_security_group.sh <your-ecs-ip> [region]}"
REGION="${2:-cn-beijing}"

echo "=== ClawShell ECS 安全组配置 ==="
echo "ECS IP: $ECS_IP"
echo "Region: $REGION"

# 检查 aliyun CLI
if ! command -v aliyun &>/dev/null; then
    echo "❌ aliyun CLI 未安装"
    echo "安装: pip install aliyun-python-sdk-ecs"
    exit 1
fi

# 安全组名称
SG_NAME="clawshell-ecs-sg"

echo ""
echo "正在查询安全组..."

# 获取安全组 ID（需要替换为你的安全组 ID）
# SG_ID=$(aliyun ecs DescribeSecurityGroups --RegionId $REGION --SecurityGroupName $SG_NAME --query 'SecurityGroups.SecurityGroup[0].SecurityGroupId' --output json)

echo ""
echo "=== 需要手动配置的安全组规则 ==="
echo ""
echo "在阿里云 ECS 控制台 → 安全组 → 配置以下规则："
echo ""
echo "入方向规则："
echo "+--------+-------------+--------+-----------+-------+------+"
echo "| 协议   | 端口范围   | 来源   | 描述       | 策略  | 优先级|"
echo "+--------+-------------+--------+-----------+-------+------+"
echo "| TCP    | 22/22      | 你的IP | SSH        | 接受  | 1    |"
echo "| TCP    | 8080/8080  | 0.0.0.0| WS Edge   | 接受  | 100  |"
echo "| TCP    | 8081/8081  | 127... | MCP Local | 接受  | 100  |"
echo "| TCP    | 8082/8082  | 127... | HTTP Local| 接受  | 100  |"
echo "+--------+-------------+--------+-----------+-------+------+"
echo ""
echo "出方向规则（默认全接受，如有限制需添加）："
echo "+--------+-------------+--------+-----------+-------+------+"
echo "| 协议   | 端口范围   | 目标   | 描述       | 策略  | 优先级|"
echo "+--------+-------------+--------+-----------+-------+------+"
echo "| TCP    | 443/443    | 0.0.0.0| HTTPS API | 接受  | 100  |"
echo "+--------+-------------+--------+-----------+-------+------+"
echo ""
echo "自动配置命令（如果你知道安全组 ID）："
echo "aliyun ecs AuthorizeSecurityGroup \\"
echo "  --RegionId $REGION \\"
echo "  --SecurityGroupId <SG_ID> \\"
echo "  --IpProtocol tcp \\"
echo "  --PortRange '8080/8080' \\"
echo "  --SourceCidrIp '0.0.0.0/0' \\"
echo "  --Description 'ClawShell WS Edge'"

echo ""
echo "=== 验证端口连通性 ==="
for PORT in 22 8080 8081 8082; do
    if nc -z -w3 $ECS_IP $PORT 2>/dev/null; then
        echo "  ✅ Port $PORT 开放"
    else
        echo "  ❌ Port $PORT 无法连接"
    fi
done
