package io.ctsi.tenet.framework.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author Mc.D
 */

@ConfigurationProperties(prefix = "tenet.schedule")
@Getter
@Setter
public class TenetNodeScheduleConfig {
    /**
     * 超过该线程数时调度器会自动降速
     */
    private int maxThread = 200;

    /**
     * 核心线程数
     */
    private int defaultThread = 20;

    /**
     * 加速步长（倍率）,大于0,小于1
     */
    private double defaultFasterStep = 0.8;

    /**
     * 减速步长（倍率）,大于1
     */
    private double defaultSlowdownStep = 1.2;

    /**
     * 自动计算加减速倍率
     */
    private boolean autoCalcStep = true;

    /**
     * 初始频率（ms）
     */
    private long defaultRate = 50;

    /**
     * 是否开启动态调度
     */
    private boolean activeSchedule = true;

    /**
     * 是否允许超载
     */
    private boolean alowOverloading = false;

    /**
     * 进入超载前需满载时间（秒）
     */
    private int fullTimeBeforeOverload = 300;


    /**
     * 超载百分比
     */
    private int overloadPercent = 130;

    /**
     * 是否允许空载休眠
     */
    private boolean alowEmptyloading = true;

    /**
     * 进入空载休眠前需空载时间（秒）
     */
    private int emptyTimeBeforeEmptyload = 60;
}
