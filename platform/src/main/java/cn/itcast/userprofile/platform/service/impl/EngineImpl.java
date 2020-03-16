package cn.itcast.userprofile.platform.service.impl;

import cn.itcast.up.common.OozieParam;
import cn.itcast.up.common.OozieUtils;
import cn.itcast.userprofile.platform.bean.dto.ModelDto;
import cn.itcast.userprofile.platform.service.Engine;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class EngineImpl implements Engine {
    @Override
    public String startModel(ModelDto model) {
        // 设置动态的参数, 例如如何调度, 主类名, jar 的位置
        OozieParam param = new OozieParam(
                model.getId(),
                model.getMainClass(),
                model.getPath(),
                model.getArgs(),
                ModelDto.Schedule.formatTime(model.getSchedule().getStartTime()),
                ModelDto.Schedule.formatTime(model.getSchedule().getEndTime())
        );

        // 生成配置,启动oozie任务的核心参数.
        Properties properties = OozieUtils.genProperties(param);

        // 上传各种配置, workflow.xml, coordinator.xml
        OozieUtils.uploadConfig(model.getId());

        // 因为如果不保留一份 job.properties 的文件, 无法调试错误
        OozieUtils.store(model.getId(), properties);

        // 运行 Oozie 任务
        String jobId = OozieUtils.start(properties);

        return jobId;
    }

    @Override
    public void stopModel(ModelDto model) {
        OozieUtils.stop(model.getName());
    }
}
