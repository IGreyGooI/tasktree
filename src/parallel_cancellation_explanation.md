# 并行节点取消传播机制

## 当前行为问题

当并行节点被取消时，**当前实现有一个设计问题**：

```rust
// 在 parallel.rs 中
let child_ctx = ctx.child_scope(); // ❌ 问题：创建独立的取消作用域
```

这意味着：
- 父节点取消 **不会** 传播到子任务
- 子任务会继续运行，即使父节点已经取消

## 正确的行为应该是

1. **层次化取消传播**：
   ```rust
   let child_ctx = ctx.child_linked(); // ✅ 正确：创建链接的取消作用域
   ```

2. **协作式取消**：
   - 父节点取消 → 自动取消所有子token
   - 子任务检查自己的token，发现被取消后优雅退出
   - 不使用 `abort_all()` 强制中止

## 取消流程

```
外部取消请求
    ↓
并行节点的token被取消
    ↓ (通过child_token()自动传播)
所有子任务的token被取消
    ↓ (子任务检查token)
子任务返回 Failure
    ↓
并行节点收集结果并返回
```

## 需要修复

1. 使用 `child_linked()` 而不是 `child_scope()`
2. 移除 `join_set.abort_all()` 强制中止
3. 让子任务自然地通过token检查退出