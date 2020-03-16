package cn.itcast.userprofile.platform.service.impl;

import cn.itcast.userprofile.platform.bean.dto.ModelDto;
import cn.itcast.userprofile.platform.bean.dto.TagDto;
import cn.itcast.userprofile.platform.bean.dto.TagModelDto;
import cn.itcast.userprofile.platform.bean.po.ModelPo;
import cn.itcast.userprofile.platform.bean.po.TagPo;
import cn.itcast.userprofile.platform.repo.ModelRepository;
import cn.itcast.userprofile.platform.repo.TagRepository;
import cn.itcast.userprofile.platform.service.Engine;
import cn.itcast.userprofile.platform.service.TagService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class TagServiceImpl implements TagService {
    @Autowired
    private TagRepository tagRepo;
    @Autowired
    private ModelRepository modelRepo;
    @Autowired
    private Engine engine;


    /**
     * 将前台传递过来的1/2/3级标签保存到数据库
     * @param tags
     */
    @Override
    public void saveTags(List<TagDto> tags) {
        // TagDto(id=null, name=金融, rule=null, level=1, pid=null),
        // TagDto(id=null, name=黑马银行, rule=null, level=2, pid=null),
        // TagDto(id=null, name=人口属性, rule=null, level=3, pid=null)

        //注意:我们现在手里的是DTO传输对象,而数据库要的是PO持久对象
        //实际中一般需要进行这样的区分,原因是防止前台从输入参数推测后端数据库结构然后攻击数据库
        //所以一般需要把传输对象和持久化对象做一个区分

        //1.将DTO转为PO方便后面进行持久化保存
        TagPo tagPo1 = this.convert(tags.get(0));
        TagPo tagPo2 = this.convert(tags.get(1));
        TagPo tagPo3 = this.convert(tags.get(2));

        //定义一个临时变量,来保存ID
        TagPo tmp = null;

        //2.先保存1级标签
        TagPo  tagResult  = tagRepo.findByNameAndLevelAndPid(tagPo1.getName(), tagPo1.getLevel(),tagPo1.getPid());
        if (tagResult == null){//没查到
            tmp = tagRepo.save(tagPo1);
        }else{//查到了,应该取出id作为下一级标签的父id
            tmp = tagResult;
        }
        //3.将1级标签的id作为2级标签的pid,再保存2级标签
        TagPo  tagResult2  = tagRepo.findByNameAndLevelAndPid(tagPo2.getName(), tagPo2.getLevel(),tagPo2.getPid());
        if(tagResult2 == null){
            tagPo2.setPid(tmp.getId());
            tmp = tagRepo.save(tagPo2);
        }else{
            tmp = tagResult2;
        }
        //4.再将2级标签的id作为3级标签的pid,再保存3级标签
        TagPo  tagResult3  = tagRepo.findByNameAndLevelAndPid(tagPo3.getName(), tagPo3.getLevel(),tagPo3.getPid());
        if(tagResult3 == null){
            tagPo3.setPid(tmp.getId());
            tagRepo.save(tagPo3);
        }
    }


    @Override
    public List<TagDto> findByPid(Long pid) {
        List<TagPo> list = tagRepo.findByPid(pid);
        //方式一:java8之前的写法
        //将每一个Po转为Dto,并放入List中,最后将list返回
        /*List<TagDto> listnew = new ArrayList<>();
        for (TagPo tagPo : list) {
            TagDto tagDto = this.convert(tagPo);
            listnew.add(tagDto);
        }*/

        //方式二:使用Java8的Lambda表达式(函数式编程)+StreamAPI
        /*List<TagDto> listnew = list.stream().map((po) -> {
            TagDto tagDto = this.convert(po);
            return tagDto;
        }).collect(Collectors.toList());*/

        /*List<TagDto> listnew = list
                .stream()
                .map(po -> this.convert(po))
                .collect(Collectors.toList());*/
        /*List<TagDto> listnew = list
                .stream()
                .map(this::convert)//行为参数化
                .collect(Collectors.toList());*/
        //return listnew;

        return list.stream().map(this::convert).collect(Collectors.toList());
    }

    @Override
    public List<TagDto> findByLevel(Integer level) {
        List<TagPo> list = tagRepo.findByLevel(level);
        List<TagDto> listDto = list.stream().map(this::convert).collect(Collectors.toList());
        return listDto;
    }


    @Override
    public void addTagModel(TagDto tagDto, ModelDto modelDto) {
        //保存tag
        TagPo tagPo = tagRepo.save(convert(tagDto));
        //保存model
        modelRepo.save(convert(modelDto, tagPo.getId()));
    }

    @Override
    public List<TagModelDto> findModelByPid(Long pid) {
        List<TagPo> tagPos = tagRepo.findByPid(pid);
        return tagPos.stream().map((tagPo) -> {
            Long id = tagPo.getId();
            ModelPo modelPo = modelRepo.findByTagId(id);
            if (modelPo == null) {
                //找不到model,就只返回tag
                return new TagModelDto(convert(tagPo),null);
            }
            return new TagModelDto(convert(tagPo), convert(modelPo));
        }).collect(Collectors.toList());
    }

    @Override
    public void addDataTag(TagDto tagDto) {
        tagRepo.save(convert(tagDto));
    }


    @Override
    public void updateModelState(Long id, Integer state) {
        String jobId = null;
        ModelPo modelPo = modelRepo.findByTagId(id);
        //如果传递过来的状态是3,那么就是启动,如果是4那么就是停止
        if (state == ModelPo.STATE_ENABLE) {
            //启动流程
            jobId = engine.startModel(convert(modelPo));
        }
        if (state == ModelPo.STATE_DISABLE) {
            //关闭流程
            engine.stopModel(convert(modelPo));
        }
        //更新状态信息
        modelPo.setState(state);
        modelPo.setName(jobId);
        modelRepo.save(modelPo);
    }



    private ModelDto convert(ModelPo modelPo) {
        ModelDto modelDto = new ModelDto();
        modelDto.setId(modelPo.getId());
        modelDto.setName(modelPo.getName());
        modelDto.setMainClass(modelPo.getMainClass());
        modelDto.setPath(modelPo.getPath());
        modelDto.setArgs(modelPo.getArgs());
        modelDto.setState(modelPo.getState());
        modelDto.setSchedule(modelDto.parseDate(modelPo.getSchedule()));
        return modelDto;
    }

    /**
     * modelDto转为modelPo
     * @param modelDto
     * @param id
     * @return
     */
    private ModelPo convert(ModelDto modelDto, Long id) {
        ModelPo modelPo = new ModelPo();
        modelPo.setId(modelDto.getId());
        modelPo.setTagId(id);
        modelPo.setName(modelDto.getName());
        modelPo.setMainClass(modelDto.getMainClass());
        modelPo.setPath(modelDto.getPath());
        modelPo.setSchedule(modelDto.getSchedule().toPattern());
        modelPo.setCtime(new Date());
        modelPo.setUtime(new Date());
        modelPo.setState(modelDto.getState());
        modelPo.setArgs(modelDto.getArgs());
        return modelPo;
    }



    /**
     * po转换为dto对象
     * @param tagPo
     * @return
     */
    private TagDto convert(TagPo tagPo){
        TagDto tagDto = new TagDto();
        tagDto.setId(tagPo.getId());
        tagDto.setLevel(tagPo.getLevel());
        tagDto.setName(tagPo.getName());
        tagDto.setPid(tagPo.getPid());
        tagDto.setRule(tagPo.getRule());
        return tagDto;
    }


    /**
     * 将TagDto转换为TagPo对象
     * @return
     */
    private TagPo convert(TagDto tagDto) {
        TagPo tagPo = new TagPo();
        tagPo.setId(tagDto.getId());
        tagPo.setName(tagDto.getName());
        tagPo.setRule(tagDto.getRule());
        tagPo.setLevel(tagDto.getLevel());
        if (tagDto.getLevel() == 1) {
            //如果当前等级为1级,那么设置父ID为-1
            tagPo.setPid(-1L);
        } else {
            tagPo.setPid(tagDto.getPid());
        }
        tagPo.setCtime(new Date());
        tagPo.setUtime(new Date());
        return tagPo;
    }



}
