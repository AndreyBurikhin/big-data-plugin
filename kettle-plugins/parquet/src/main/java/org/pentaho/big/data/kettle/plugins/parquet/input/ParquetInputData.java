package org.pentaho.big.data.kettle.plugins.parquet.input;

import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.fileinput.BaseFileInputStep;
import org.pentaho.di.trans.steps.fileinput.BaseFileInputStepData;

public class ParquetInputData extends BaseFileInputStepData {
  ParquetInputFormat input;
  List<InputSplit> splits;
  int currentSplit;
  RecordReader reader;
  RowMetaInterface outputRowMeta;
}
