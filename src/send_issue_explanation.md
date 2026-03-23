# Send Trait 问题解释

## 为什么这段代码不工作

```rust
let child_name = child.name().to_string();

join_set.spawn(async move {
    trace!("Parallel child {} ({}) starting", i, child_name);
    let result = child_ctx.execute(&child_clone).await;
    trace!("Parallel child {} ({}) completed: {:?}", i, child_name, result);
    (i, result)
});
```

**错误信息**: `future cannot be sent between threads safely`

## 问题根源

### 1. Send Trait 要求
- `tokio::spawn` 要求 Future 必须实现 `Send` trait
- `Send` 意味着该类型可以安全地在线程间传递
- 如果 Future 内部包含非 `Send` 的类型，整个 Future 就不是 `Send`

### 2. 可能的原因

#### A. 递归 Async 函数问题
```rust
// AsyncExecutionContext::execute 是递归的 async 函数
pub async fn execute(&self, node: &BehaviorTreeNode) -> NodeResult {
    // ... 递归调用
    child_ctx.execute(child).await
}
```
- 递归 async 函数创建自引用的 Future
- 这种 Future 往往不是 `Send`

#### B. Trait Object 问题
```rust
// BehaviorTreeNode 包含 trait objects
BehaviorTreeNode::Action(action) => {
    action.execute(child_context).await  // action: Arc<dyn AsyncBehaviorNode>
}
```
- `dyn AsyncBehaviorNode` 可能不是 `Send`
- 需要明确标记: `dyn AsyncBehaviorNode + Send`

#### C. 生命周期和借用问题
```rust
// 在 async move 块中
let result = child_ctx.execute(&child_clone).await;
//                              ^^^^^^^^^^ 这个引用可能造成问题
```

## 解决方案

### 方案1: 简化为顺序执行
```rust
// 暂时不用真正的并行，顺序执行所有子节点
for child in children {
    let result = child_ctx.execute(child).await;
    // 处理结果...
}
```

### 方案2: 使用 tokio::select!
```rust
// 不用 spawn，使用 select! 实现并发
tokio::select! {
    result1 = child1_ctx.execute(&child1) => { /* handle */ }
    result2 = child2_ctx.execute(&child2) => { /* handle */ }
    // ...
}
```

### 方案3: 修复 Send 问题
```rust
// 确保所有类型都是 Send
pub trait AsyncBehaviorNode: Send + Sync + Debug + 'static {
    // ...
}

// 使用 Box::pin 处理递归
pub fn execute(&self, node: &BehaviorTreeNode) -> Pin<Box<dyn Future<Output = NodeResult> + Send + '_>> {
    Box::pin(async move { /* ... */ })
}
```

## 当前情况

目前最简单的解决方案是**方案1**，先实现功能，然后再优化并发性能。