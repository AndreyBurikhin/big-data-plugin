package org.pentaho.big.data.kettle.plugins.parquet.output;

import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

@Step( id = "ParquetOutput", image = "HBO.svg", name = "HBaseOutput.Name", description = "HBaseOutput.Description",
    categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.BigData",
    documentationUrl = "http://wiki.pentaho.com/display/EAI/HBase+Output",
    i18nPackageName = "org.pentaho.di.trans.steps.parquet" )
public class ParquetOutputMeta extends BaseStepMeta implements StepMetaInterface {

  @Override
  public void setDefault() {
    // TODO Auto-generated method stub
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {
    return new ParquetOutput( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override
  public StepDataInterface getStepData() {
    return new ParquetOutputData();
  }

}
