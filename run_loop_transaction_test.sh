#!/bin/bash

# 启用 core dump
ulimit -c unlimited

run_count=0
max_runs=100

while [ $run_count -lt $max_runs ]; do
    run_count=$((run_count + 1))
    echo ""
    echo "=========================================="
    echo "🔄 Run #$run_count / $max_runs"
    echo "=========================================="
    
    python tests/transaction_test.py 2>&1 | tee test_run_${run_count}.log
    exit_code=${PIPESTATUS[0]}
    
    if [ $exit_code -ne 0 ]; then
        echo ""
        echo "❌ Test FAILED with exit code: $exit_code at run #$run_count"
        
        # 检查日志中是否有 SIGABRT
        if grep -q "Process has terminated with code -6" test_run_${run_count}.log; then
            echo "🔴 SIGABRT DETECTED!"
            echo ""
            echo "📋 Error details:"
            grep -A 10 "Process has terminated with code -6" test_run_${run_count}.log
        fi
        
        # 检查是否有异常终止信息
        if grep -q "terminate called" test_run_${run_count}.log; then
            echo "🔴 C++ exception detected: terminate called without an active exception"
            echo ""
            echo "📋 Full error context:"
            grep -B 5 -A 10 "terminate called" test_run_${run_count}.log
        fi
        
        # 检查是否有 crash 分析文件
        if ls crash_analysis_*.log 1> /dev/null 2>&1; then
            echo ""
            echo "✅ Found crash analysis file:"
            ls -lh crash_analysis_*.log
            echo ""
            echo "📄 Crash analysis content:"
            cat crash_analysis_*.log
        fi
        
        echo ""
        echo "💾 Full log saved to: test_run_${run_count}.log"
        echo "🛑 Stopping test loop due to failure"
        exit 1
    fi
    
    echo "✅ Run #$run_count completed successfully"
    # 清理成功的日志以节省空间
    rm -f test_run_${run_count}.log
done

echo ""
echo "🎉 All $max_runs runs completed successfully!"