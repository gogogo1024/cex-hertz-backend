package service

// GlobalMatchEngine 是进程级的 MatchEngine 实例指针，由 main 在启动时注入
// 供 HTTP handler 或其他包访问，用于将外部请求转发到撮合引擎。
var GlobalMatchEngine *PartitionAwareMatchEngine
