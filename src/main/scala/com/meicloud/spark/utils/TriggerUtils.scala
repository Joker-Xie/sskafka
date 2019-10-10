package com.meicloud.spark.utils

import org.apache.spark.sql.streaming.Trigger

/**
  * Created by yesk on 2019-3-12. 
  */
object TriggerUtils {
  def getTrigger(trigger:Int):Trigger={
    if(trigger>1) Trigger.ProcessingTime(trigger+" "+ConstantUtils.TRIGGER_UNIT_SECONDS)
    else if(trigger==1) Trigger.ProcessingTime(trigger+" "+ConstantUtils.TRIGGER_UNIT_SECOND)
    else  Trigger.Once()
  }

}
