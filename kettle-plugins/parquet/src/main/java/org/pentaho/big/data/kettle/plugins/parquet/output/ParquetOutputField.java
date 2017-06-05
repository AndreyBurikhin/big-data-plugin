package org.pentaho.big.data.kettle.plugins.parquet.output;

import org.apache.avro.Schema;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;

public class ParquetOutputField {

  private String name;
  
  private int type;

  public String getName() {
    return name;
  }

  public void setName( String name ) {
    this.name = name;
  }

  public int getType() {
    return type;
  }

  public void setType( int type ) {
    this.type = type;
  }
  
  public String getTypeDesc() {
    return ValueMetaFactory.getValueMetaName( type );
  }
  
  public void setType( String typeDesc ) {
    this.type = ValueMetaFactory.getIdForValueMeta( typeDesc );
  }

  public static Schema.Type getDefaultAvroType( int pentahoType ) {
    switch ( pentahoType ) {
      case ValueMetaInterface.TYPE_NUMBER:
      case ValueMetaInterface.TYPE_BIGNUMBER:
        return Schema.Type.DOUBLE;
      case ValueMetaInterface.TYPE_INTEGER:
        return Schema.Type.LONG;
      case ValueMetaInterface.TYPE_BOOLEAN:
        return Schema.Type.BOOLEAN;
      default:
        return Schema.Type.STRING;
    }
  }
  
}
