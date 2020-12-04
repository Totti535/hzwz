package com.hzwz.tailbase.backendprocess;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.hzwz.tailbase.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.hzwz.tailbase.Constants.PROCESS_COUNT;

@RestController
public class BackendController {

    private static final Logger LOGGER = LoggerFactory.getLogger(BackendController.class.getName());

    // FINISH_PROCESS_COUNT will add one, when process call finish();
    private static volatile Integer FINISH_PROCESS_COUNT = 0;

    // single thread to run, do not use lock
    private static Map<Integer, Integer> CURRENT_BATCH = new ConcurrentHashMap<>();

    // save 90 batch for wrong trace
    private static int BATCH_COUNT = 90;
    private static Map<Integer, List<TraceIdBatch>> TRACEID_BATCH_LIST = new HashMap<>();

    public static void init() {
        for (int i = 0; i < Constants.NUMBER_OF_THREAD; i++) {
            List<TraceIdBatch> list = new ArrayList<>();
            for (int j = 0; j < BATCH_COUNT; j++) {
                TraceIdBatch traceIdBatch = new TraceIdBatch();
                traceIdBatch.setThreadNumber(i);
                list.add(traceIdBatch);
            }
            TRACEID_BATCH_LIST.put(i, list);

            CURRENT_BATCH.put(i, 0);
        }
    }


    @RequestMapping("/setWrongTraceId")
    public String setWrongTraceId(@RequestParam String traceIdListJson, @RequestParam int batchPos, @RequestParam int threadNumber) {
        int pos = batchPos % BATCH_COUNT;
        List<String> traceIdList = JSON.parseObject(traceIdListJson, new TypeReference<List<String>>() {
        });
        LOGGER.info(String.format("setWrongTraceId had called, batchPos:%d, traceIdList:%s", batchPos, traceIdListJson));
        TraceIdBatch traceIdBatch = TRACEID_BATCH_LIST.get(threadNumber).get(pos);
        if (traceIdBatch.getBatchPos() != 0 && traceIdBatch.getBatchPos() != batchPos) {
            LOGGER.warn("overwrite traceId batch when call setWrongTraceId");
        }

        if (traceIdList != null && traceIdList.size() > 0) {
            traceIdBatch.setBatchPos(batchPos);
            traceIdBatch.setProcessCount(traceIdBatch.getProcessCount() + 1);
            traceIdBatch.getTraceIdList().addAll(traceIdList);
        }
        return "suc";
    }

    @RequestMapping("/finish")
    public String finish() {
        FINISH_PROCESS_COUNT++;
        LOGGER.warn("receive call 'finish', count:" + FINISH_PROCESS_COUNT);
        return "suc";
    }

    /**
     * trace batch will be finished, when client process has finished.(FINISH_PROCESS_COUNT == PROCESS_COUNT)
     *
     * @return
     */
    public static boolean isFinished() {
        for (Map.Entry<Integer, List<TraceIdBatch>> entry : TRACEID_BATCH_LIST.entrySet()) {
            for (int i = 0; i < BATCH_COUNT; i++) {
                TraceIdBatch currentBatch = entry.getValue().get(i);
                if (currentBatch.getBatchPos() != 0) {
                    return false;
                }
            }
        }

        if (FINISH_PROCESS_COUNT < Constants.PROCESS_COUNT) {
            return false;
        }
        return true;
    }

    /**
     * get finished bath when current and next batch has all finished
     *
     * @return
     */
    public static List<TraceIdBatch> getFinishedBatch() {
        List<TraceIdBatch> traceIdBatches = new ArrayList<>();
        for (Map.Entry<Integer, List<TraceIdBatch>> entry : TRACEID_BATCH_LIST.entrySet()) {
            int next = CURRENT_BATCH.get(entry.getKey()) + 1;
            if (next >= BATCH_COUNT) {
                next = 0;
            }
            TraceIdBatch nextBatch = entry.getValue().get(next);
            TraceIdBatch currentBatch = entry.getValue().get(CURRENT_BATCH.get(entry.getKey()));
            // when client process is finished, or then next trace batch is finished. to get checksum for wrong traces.
            if ((FINISH_PROCESS_COUNT >= Constants.PROCESS_COUNT && currentBatch.getBatchPos() > 0) ||
                    (nextBatch.getProcessCount() >= PROCESS_COUNT && currentBatch.getProcessCount() >= PROCESS_COUNT)) {
                // reset
                TraceIdBatch newTraceIdBatch = new TraceIdBatch();
                newTraceIdBatch.setThreadNumber(entry.getKey());

                entry.getValue().set(CURRENT_BATCH.get(entry.getKey()), newTraceIdBatch);
                CURRENT_BATCH.put(entry.getKey(), next);
                traceIdBatches.add(currentBatch);
            }
        }
        return traceIdBatches;
    }

}