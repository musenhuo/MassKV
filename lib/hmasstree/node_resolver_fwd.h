/* FlowKV - H-Masstree External Storage Extension
 * Copyright (c) 2026 FlowKV Authors
 *
 * NodeResolver forward declarations
 * This file can be included without causing circular dependencies
 */
#ifndef HMASSTREE_NODE_RESOLVER_FWD_H
#define HMASSTREE_NODE_RESOLVER_FWD_H

#ifdef HMASSTREE_EXTERNAL_STORAGE

namespace Masstree {

// Forward declaration of NodeResolver
template <typename P> class NodeResolver;

/**
 * @brief Global accessor for per-thread NodeResolver
 *
 * Similar to IndexStorageRegistry, provides thread-local access to resolver
 */
template <typename P>
class NodeResolverRegistry {
public:
    static NodeResolver<P>* get() { return tls_resolver_; }
    static void set(NodeResolver<P>* resolver) { tls_resolver_ = resolver; }
    
private:
    static inline thread_local NodeResolver<P>* tls_resolver_ = nullptr;
};

}  // namespace Masstree

#endif  // HMASSTREE_EXTERNAL_STORAGE

#endif  // HMASSTREE_NODE_RESOLVER_FWD_H
