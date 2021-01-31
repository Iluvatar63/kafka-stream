package com.coding4fun.kafka.stream;

import com.coding4fun.kafka.models.*;

import org.apache.kafka.streams.kstream.ValueJoiner;

public class EndOfCureJoiner implements ValueJoiner<EndOfCure, ShiftChanged, EnhancedEndOfCure> {

    public EnhancedEndOfCure apply(EndOfCure endOfCure, ShiftChanged shiftChanged) {
      return EnhancedEndOfCure.newBuilder()
          .setDate(endOfCure.getDate())
          .setItemCode(endOfCure.getItemCode())
          .setCureEquipmentId(endOfCure.getCureEquipmentId())
          .setShiftCode(shiftChanged.getShiftCode())
          .build();
    }
  }