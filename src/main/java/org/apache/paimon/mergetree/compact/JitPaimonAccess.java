/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0.
 */
package org.apache.paimon.mergetree.compact;

import org.apache.paimon.io.KeyValueFileWriterFactory;

/**
 * Reads the writer factory of Paimon's default merge-tree rewriter. {@link CompactRewriterFactory}
 * hands a replacement only the default rewriter, and the factory it writes through is a protected
 * field. A small accessor upstream would make this class unnecessary (see docs/ALIGNMENT.md).
 */
public final class JitPaimonAccess {

    /** Whether {@code rewriter} is exactly Paimon's plain merge-tree rewriter, not a subclass. */
    public static boolean isPlainMergeTreeRewriter(CompactRewriter rewriter) {
        return rewriter.getClass() == MergeTreeCompactRewriter.class;
    }

    public static KeyValueFileWriterFactory writerFactory(CompactRewriter rewriter) {
        return ((MergeTreeCompactRewriter) rewriter).writerFactory;
    }

    private JitPaimonAccess() {}
}
