package org.pentaho.big.data.kettle.plugins.parquet.input;

import java.util.List;

import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.fileinput.BaseFileInputStepMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

@Step( id = "ParquetInput", image = "HBO.svg", name = "ParquetInput.Name", description = "ParquetInput.Description",
    categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.BigData",
    documentationUrl = "http://wiki.pentaho.com/display/EAI/HBase+Input",
    i18nPackageName = "org.pentaho.di.trans.steps.parquet", isSeparateClassLoaderNeeded = true )
public class ParquetInputMeta extends
    BaseFileInputStepMeta<BaseFileInputStepMeta.AdditionalOutputFields, BaseFileInputStepMeta.InputFiles<ParquetInputField>> {

  protected String dir;

  public ParquetInputMeta() {
    additionalOutputFields = new BaseFileInputStepMeta.AdditionalOutputFields();
    inputFiles = new BaseFileInputStepMeta.InputFiles<>();
    inputFiles.inputFields = new ParquetInputField[0];
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {
    return new ParquetInput( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override
  public StepDataInterface getStepData() {
    return new ParquetInputData();
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder( 1500 );

    retval.append( "    " ).append( XMLHandler.addTagValue( "dir", dir ) );

    return retval.toString();
  }

  @Override
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    dir = XMLHandler.getTagValue( stepnode, "dir" );
  }

  /**
   * TODO: remove from base
   */
  @Override
  public String getEncoding() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setDefault() {
    // TODO Auto-generated method stub

  }
}
