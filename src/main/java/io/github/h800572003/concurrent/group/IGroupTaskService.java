package io.github.h800572003.concurrent.group;

import java.util.List;

/**
 * 任務拆分服務
 */
public interface IGroupTaskService {

    /**
     * 任務執行
     * @param groupTaskOption
     * @param <T>
     */
    <T> void execute(GroupTaskOption<T> groupTaskOption);



    interface IGroupTask<T> {
        void run(List<T> tasks);
    }
}
