package io.github.h800572003.concurrent;

import io.github.h800572003.concurrent.group.GroupTaskOption;
import io.github.h800572003.concurrent.group.GroupTaskService;
import io.github.h800572003.concurrent.group.IGroupTaskService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
class GroupTaskServiceTest {
    IGroupTaskService.IGroupTask<Integer> run;

    @BeforeEach
    void init() {
        run = Mockito.spy(new IGroupTaskService.IGroupTask<Integer>() {

            @Override
            public void run(List<Integer> tasks) {
                log.info("start size:{} :{}", tasks.size(), tasks);
                log.info("end");
            }
        });

    }

    /**
     * 給 0~10任務
     * 分3組人執行
     * 總共執行3次任務
     */
    @Test
    void execute() {
        exe(3, 2, 10);

        Mockito.verify(run, Mockito.times(3)).run(Mockito.anyList());


    }

    @Test
    void execute100() {
        exe(3, 2, 1000);
        Mockito.verify(run, Mockito.times(3)).run(Mockito.anyList());

    }

    @Test
    @Timeout(3)
    void executeInterrupt() throws InterruptedException {

       this.run = Mockito.spy(new IGroupTaskService.IGroupTask<Integer>() {

            @Override
            public void run(List<Integer> tasks) {
                log.info(Thread.currentThread().getName()+" start size:{} :{}", tasks.size(), tasks);

                try {
                    TimeUnit.MILLISECONDS.sleep(300);
                } catch (InterruptedException e) {
                   log.error("InterruptedException");
                }

                log.info(Thread.currentThread().getName()+" end");
            }
        });

        Thread thread = new Thread(() -> exe(10, 2, 300));
        thread.start();


        TimeUnit.MILLISECONDS.sleep(400);

        thread.interrupt();

        thread.join();
        Mockito.verify(run, Mockito.atLeast(2)).run(Mockito.anyList());


        log.info("ok");


    }


    private IGroupTaskService.IGroupTask<Integer> exe(int groupSize, int threadSize, int dataSize) {
        GroupTaskService groupTaskService = new GroupTaskService();
        //init task


        //GIVE
        final List<Integer> collect = IntStream.range(0, dataSize)//
                .boxed()//
                .collect(Collectors.toList());//


        GroupTaskOption.GroupTaskOptionBuilder<Integer> groupTaskOptionBuilder = new GroupTaskOption.GroupTaskOptionBuilder();
        GroupTaskOption<Integer> input = groupTaskOptionBuilder
                .name("test")//
                .threadSize(threadSize)//
                .groupSize(groupSize)//
                .task(run)//
                .tasks(collect)//
                .build();

        //when
        groupTaskService.execute(input);


        return run;
    }


}