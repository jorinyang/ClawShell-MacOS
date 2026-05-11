"""
ClawShell Cloud Hub — 协议层
定义跨端通信的协议常量和消息类型
"""

# ─── 消息类型 ────────────────────────────────────────────────────────────────

MSG_TYPE_AUTH          = "auth"          # 认证帧
MSG_TYPE_AUTH_OK       = "auth_ok"       # 认证成功
MSG_TYPE_MCP_REQUEST   = "mcp_request"   # MCP 请求
MSG_TYPE_MCP_RESPONSE  = "mcp_response"  # MCP 响应
MSG_TYPE_PING          = "ping"          # 心跳
MSG_TYPE_PONG          = "pong"          # 心跳响应
MSG_TYPE_BROADCAST     = "broadcast"     # 广播
MSG_TYPE_DISPATCH      = "dispatch"      # 云端调度指令（推送给端侧）
MSG_TYPE_CLAIM         = "claim"         # 端侧认领任务
MSG_TYPE_CLAIM_OK      = "claim_ok"     # 认领成功
MSG_TYPE_CLAIM_REJECT  = "claim_reject" # 认领被拒绝
MSG_TYPE_TASK_EVENT    = "task_event"    # 任务状态变更推送
MSG_TYPE_EDGE_REGISTER = "edge_register" # 端侧注册（携带能力声明）
MSG_TYPE_EDGE_INFO     = "edge_info"     # 云端返回端侧节点信息
MSG_TYPE_ECHO          = "echo"          # 回显

# ─── MCP 方法前缀 ───────────────────────────────────────────────────────────

METHOD_PREFIX_MEMORY   = "memory_"    # 记忆域
METHOD_PREFIX_KNOWLEDGE = "knowledge_" # 知识域
METHOD_PREFIX_KANBAN   = "kanban_"    # 任务域
METHOD_PREFIX_SKILL    = "skill_"     # 技能域
METHOD_PREFIX_NODE     = "node_"      # 节点管理域
METHOD_PREFIX_VAULT   = "vault_"     # 文件存储域

# ─── 任务调度模式 ────────────────────────────────────────────────────────────

DISPATCH_MODE_CLOUD_ASSIGN = "cloud_assign"  # 云端指定端侧
DISPATCH_MODE_OPEN_CLAIM   = "open_claim"    # 开放认领
DISPATCH_MODE_BROADCAST    = "broadcast"      # 广播给所有端

# ─── 任务状态 ────────────────────────────────────────────────────────────────

TASK_STATUS_PENDING   = "pending"   # 待认领
TASK_STATUS_ASSIGNED  = "assigned"  # 已分配给端侧
TASK_STATUS_CLAIMED   = "claimed"  # 端侧已认领
TASK_STATUS_WORKING   = "working"  # 执行中
TASK_STATUS_DONE      = "done"     # 已完成
TASK_STATUS_FAILED     = "failed"   # 失败
TASK_STATUS_CANCELLED  = "cancelled" # 已取消

# ─── 节点状态 ────────────────────────────────────────────────────────────────

NODE_STATUS_ONLINE  = "online"  # 在线
NODE_STATUS_BUSY    = "busy"    # 忙碌（正在执行任务）
NODE_STATUS_IDLE    = "idle"    # 空闲
NODE_STATUS_OFFLINE = "offline" # 离线
