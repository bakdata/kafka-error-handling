package com.bakdata.kafka;

import java.util.function.Predicate;
import lombok.experimental.UtilityClass;

@UtilityClass
class FilterHelper {
    static <T> Predicate<T> filterAll() {
        return ignored -> true;
    }
}
