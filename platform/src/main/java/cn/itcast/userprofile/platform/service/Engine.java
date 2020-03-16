package cn.itcast.userprofile.platform.service;


import cn.itcast.userprofile.platform.bean.dto.ModelDto;

public interface Engine {

    String startModel(ModelDto modelDto);
    void stopModel(ModelDto modelDto);
}
