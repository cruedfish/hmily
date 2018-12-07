/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.dromara.hmily.core.service.handler;

import org.aspectj.lang.ProceedingJoinPoint;
import org.dromara.hmily.common.bean.context.HmilyTransactionContext;
import org.dromara.hmily.common.bean.entity.HmilyTransaction;
import org.dromara.hmily.common.config.HmilyConfig;
import org.dromara.hmily.common.enums.HmilyActionEnum;
import org.dromara.hmily.core.concurrent.threadlocal.HmilyTransactionContextLocal;
import org.dromara.hmily.core.concurrent.threadpool.HmilyThreadFactory;
import org.dromara.hmily.core.service.HmilyTransactionHandler;
import org.dromara.hmily.core.service.executor.HmilyTransactionExecutor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * this is hmily transaction starter.
 *
 * @author xiaoyu
 */
@Component
public class StarterHmilyTransactionHandler implements HmilyTransactionHandler, ApplicationListener<ContextRefreshedEvent> {

    private final HmilyTransactionExecutor hmilyTransactionExecutor;

    private Executor executor;

    private final HmilyConfig hmilyConfig;

    @Autowired
    public StarterHmilyTransactionHandler(final HmilyTransactionExecutor hmilyTransactionExecutor, HmilyConfig hmilyConfig) {
        this.hmilyTransactionExecutor = hmilyTransactionExecutor;
        this.hmilyConfig = hmilyConfig;
    }


    @Override
    public Object handler(final ProceedingJoinPoint point, final HmilyTransactionContext context)
            throws Throwable {
        Object returnValue;
        try {
            //获取初始化的hmily事务
            HmilyTransaction hmilyTransaction = hmilyTransactionExecutor.begin(point);
            try {
                //尝试执行方法
                returnValue = point.proceed();
                //事务设置为尝试 开始准备执行分布式事务
                hmilyTransaction.setStatus(HmilyActionEnum.TRYING.getCode());
                //设置队列为更新状态
                hmilyTransactionExecutor.updateStatus(hmilyTransaction);
            } catch (Throwable throwable) {
                //如果分布式事务执行失败则取消
                //if exception ,execute cancel
                final HmilyTransaction currentTransaction = hmilyTransactionExecutor.getCurrentTransaction();
                executor.execute(() -> hmilyTransactionExecutor
                        .cancel(currentTransaction));
                throw throwable;
            }
            //execute confirm
            //如果分布式事务执行成功则确认
            final HmilyTransaction currentTransaction = hmilyTransactionExecutor.getCurrentTransaction();
            executor.execute(() -> hmilyTransactionExecutor.confirm(currentTransaction));
        } finally {
            HmilyTransactionContextLocal.getInstance().remove();
            hmilyTransactionExecutor.remove();
        }
        return returnValue;
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        if (hmilyConfig.getStarted()) {
            executor = new ThreadPoolExecutor(hmilyConfig.getAsyncThreads(),
                    hmilyConfig.getAsyncThreads(), 0, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(),
                    HmilyThreadFactory.create("hmily-execute", false),
                    new ThreadPoolExecutor.AbortPolicy());
        }

    }
}
