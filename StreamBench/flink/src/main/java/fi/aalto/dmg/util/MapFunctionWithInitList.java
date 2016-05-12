package fi.aalto.dmg.util;

import fi.aalto.dmg.frame.functions.MapWithInitListFunction;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.List;

/**
 * Created by jun on 27/02/16.
 */

public class MapFunctionWithInitList<T, R> implements MapFunction<T, R> {
    private List<T> initList;
    private MapWithInitListFunction<T, R> function;

    public MapFunctionWithInitList(MapWithInitListFunction<T, R> fun, List<T> initList) {
        this.function = fun;
        this.initList = initList;
    }

    @Override
    public R map(T value) throws Exception {
        return function.map(value, initList);
    }
}