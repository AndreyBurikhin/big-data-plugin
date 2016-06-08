/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2016 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/
package org.pentaho.big.data.kettle.plugins.hbase.mapping;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.pentaho.big.data.api.initializer.ClusterInitializationException;
import org.pentaho.big.data.kettle.plugins.hbase.HBaseConnectionException;
import org.pentaho.big.data.kettle.plugins.hbase.input.HBaseInput;
import org.pentaho.big.data.kettle.plugins.hbase.input.MappingDefinition;
import org.pentaho.big.data.kettle.plugins.hbase.input.MappingDefinition.MappingColumn;
import org.pentaho.big.data.kettle.plugins.hbase.input.Messages;
import org.pentaho.bigdata.api.hbase.ByteConversionUtil;
import org.pentaho.bigdata.api.hbase.HBaseConnection;
import org.pentaho.bigdata.api.hbase.HBaseService;
import org.pentaho.bigdata.api.hbase.mapping.Mapping;
import org.pentaho.bigdata.api.hbase.meta.HBaseValueMetaInterface;
import org.pentaho.bigdata.api.hbase.meta.HBaseValueMetaInterfaceFactory;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;

public class MappingUtils {

  public static MappingAdmin getMappingAdmin( ConfigurationProducer cProducer ) throws HBaseConnectionException {
    HBaseConnection hbConnection = null;
    try {
      hbConnection = cProducer.getHBaseConnection();
      hbConnection.checkHBaseAvailable();
      return new MappingAdmin( hbConnection );
    } catch ( ClusterInitializationException | IOException e ) {
      throw new HBaseConnectionException( Messages.getString( "MappingDialog.Error.Message.UnableToConnect" ), e );
    }
  }

  public static MappingAdmin getMappingAdmin( HBaseService hBaseService, VariableSpace variableSpace, String siteConfig,
      String defaultConfig ) throws IOException {
    HBaseConnection hBaseConnection = hBaseService.getHBaseConnection( variableSpace, siteConfig, defaultConfig, null );
    return new MappingAdmin( hBaseConnection );
  }

  public static Mapping getMapping( MappingDefinition mappingDefinition, HBaseService hBaseService ) {
    final String tableName = mappingDefinition.getTableName();
    List<String> problems = new ArrayList<String>();
    
    // empty table name or mapping name does not force an abort
    if ( Const.isEmpty( tableName ) || Const.isEmpty( mappingDefinition.getMappingName() ) ) {
      problems.add( Messages.getString( "MappingDialog.Error.Message.MissingTableMappingName" ) );
      return null;
    }

    // do we have any non-empty rows in the table?
    if ( mappingDefinition.getMappingColumns().isEmpty() ) {
      problems.add( Messages.getString( "MappingDialog.Error.Message.NoFieldsDefined" ) );
      return null;
    }
    
    Mapping theMapping =
        hBaseService.getMappingFactory().createMapping( tableName, mappingDefinition.getMappingName() );
    boolean keyDefined = false;
    boolean moreThanOneKey = false;
    List<String> missingFamilies = new ArrayList<String>();
    List<String> missingColumnNames = new ArrayList<String>();
    List<String> missingTypes = new ArrayList<String>();

    int nrNonEmpty = mappingDefinition.getMappingColumns().size();

    // is the mapping a tuple mapping?
    boolean isTupleMapping = false;
    int tupleIdCount = 0;
    List<MappingColumn> mappingColumns = mappingDefinition.getMappingColumns();
    if ( nrNonEmpty == 5 ) {
      for ( int i = 0; i < nrNonEmpty; i++ ) {
        if ( isTupleMappingColumn( mappingColumns.get( i ).getAlias() ) ) {
          tupleIdCount++;
        }
      }
    }

    if ( tupleIdCount == 5 ) {
      isTupleMapping = true;
      theMapping.setTupleMapping( true );
    }

    for ( int i = 0; i < mappingColumns.size(); i++ ) {
      boolean isKey = false;
      String alias = null;
      if ( !Const.isEmpty( mappingColumns.get( i ).getAlias() ) ) {
        alias = mappingColumns.get( i ).getAlias();
      }
      
      isKey = mappingColumns.get( i ).isKey();

      if ( isKey && keyDefined ) {
        // more than one key, break here
        moreThanOneKey = true;
        break;
      }
      if ( isKey ) {
        keyDefined = true;
      }

      // String family = null;
      String family = "";
      if ( !Const.isEmpty( mappingColumns.get( i ).getColumnFamily() ) ) {
        family = mappingColumns.get( i ).getColumnFamily();
      } else {
        if ( !isKey && !isTupleMapping ) {
          missingFamilies.add( String.valueOf( i ) );
        }
      }
      // String colName = null;
      String colName = "";
      if ( !Const.isEmpty(  mappingColumns.get( i ).getColumnName() ) ) {
        colName = mappingColumns.get( i ).getColumnName();
      } else {
        if ( !isKey && !isTupleMapping ) {
          missingColumnNames.add( String.valueOf( i ) );
        }
      }
      String type = null;
      if ( !Const.isEmpty( mappingColumns.get( i ).getType() ) ) {
        type = mappingColumns.get( i ).getType();
      } else {
        missingTypes.add( String.valueOf( i ) );
      }
      String indexedVals = null;
      if ( !Const.isEmpty( mappingColumns.get( i ).getIndexedValues() ) ) {
        indexedVals = mappingColumns.get( i ).getIndexedValues();
      }

      HBaseValueMetaInterfaceFactory valueMetaInterfaceFactory = hBaseService.getHBaseValueMetaInterfaceFactory();
      // only add if we have all data and its all correct
      if ( isKey && !moreThanOneKey ) {
        if ( Const.isEmpty( alias ) ) {
          // pop up an error dialog - key must have an alias because it does not
          // belong to a column family or have a column name
          if ( problems != null ) {
            problems.add( Messages.getString( "MappingDialog.Error.Message.NoAliasForKey" ) );
          }
          return null;
        }

        if ( Const.isEmpty( type ) ) {
          // pop up an error dialog - must have a type for the key
          if ( problems != null ) {
            problems.add( Messages.getString( "MappingDialog.Error.Message.NoTypeForKey" ) );
          }
          return null;
        }

        if ( moreThanOneKey ) {
          // popup an error and then return
          if ( problems != null ) {
            problems.add( Messages.getString( "MappingDialog.Error.Message.MoreThanOneKey" ) );
          }
          return null;
        }

        if ( isTupleMapping ) {
          theMapping.setKeyName( alias );
          theMapping.setTupleFamilies( family );
        } else {
          theMapping.setKeyName( alias );
        }
        HBaseValueMetaInterface vm =
          valueMetaInterfaceFactory.createHBaseValueMetaInterface( null, null, alias, 0, -1, -1 );
        vm.setKey( true );
        try {
          theMapping.setKeyTypeAsString( type );
          vm.setType( HBaseInput.getKettleTypeByKeyType( theMapping.getKeyType() ) );
//          if ( includeKeyToColumns ) {
//            theMapping.addMappedColumn( vm, isTupleMapping );
//          }
        } catch ( Exception ex ) {
          // Ignore
        }
      } else {
        ByteConversionUtil byteConversionUtil = hBaseService.getByteConversionUtil();
        // don't bother adding if there are any errors
        if ( missingFamilies.size() == 0 && missingColumnNames.size() == 0 && missingTypes.size() == 0 ) {
          HBaseValueMetaInterface vm =
            valueMetaInterfaceFactory.createHBaseValueMetaInterface( family, colName, alias, 0, -1, -1 );
          try {
            vm.setHBaseTypeFromString( type );
          } catch ( IllegalArgumentException e ) {
            // TODO pop up an error dialog for this one
            return null;
          }
          if ( vm.isString() && indexedVals != null && indexedVals.length() > 0 ) {
            Object[] vals = byteConversionUtil.stringIndexListToObjects( indexedVals );
            vm.setIndex( vals );
            vm.setStorageType( ValueMetaInterface.STORAGE_TYPE_INDEXED );
          }

          try {
            theMapping.addMappedColumn( vm, isTupleMapping );
          } catch ( Exception ex ) {
            // popup an error if this family:column is already in the mapping
            // and
            // then return.
            if ( problems != null ) {
              problems.add( Messages.getString( "MappingDialog.Error.Message1.DuplicateColumn" ) + family
                  + "," + colName
                  + Messages.getString( "MappingDialog.Error.Message2.DuplicateColumn" ) );
            }

            return null;
          }
        }
      }
    }
    // now check for any errors in our Lists
    if ( !keyDefined ) {
      if ( problems != null ) {
        problems.add( Messages.getString( "MappingDialog.Error.Message.NoKeyDefined" ) );
      }
      return null;
    }

    if ( missingFamilies.size() > 0 || missingColumnNames.size() > 0 || missingTypes.size() > 0 ) {
      StringBuffer buff = new StringBuffer();
      buff.append( Messages.getString( "MappingDialog.Error.Message.IssuesPreventingSaving" ) + ":\n\n" );
      if ( missingFamilies.size() > 0 ) {
        buff.append( Messages.getString( "MappingDialog.Error.Message.FamilyIssue" ) + ":\n" );
        buff.append( missingFamilies.toString() ).append( "\n\n" );
      }
      if ( missingColumnNames.size() > 0 ) {
        buff.append( Messages.getString( "MappingDialog.Error.Message.ColumnIssue" ) + ":\n" );
        buff.append( missingColumnNames.toString() ).append( "\n\n" );
      }
      if ( missingTypes.size() > 0 ) {
        buff.append( Messages.getString( "MappingDialog.Error.Message.TypeIssue" ) + ":\n" );
        buff.append( missingTypes.toString() ).append( "\n\n" );
      }

      if ( problems != null ) {
        problems.add( buff.toString() );
      }
      return null;
    }
    return theMapping;
  }

  public static boolean isTupleMappingColumn( String columnName ) {
    return columnName.equals( Mapping.TupleMapping.KEY.toString() ) || columnName.equals( Mapping.TupleMapping.FAMILY
        .toString() ) || columnName.equals( Mapping.TupleMapping.COLUMN.toString() ) || columnName.equals(
            Mapping.TupleMapping.VALUE.toString() ) || columnName.equals( Mapping.TupleMapping.TIMESTAMP.toString() );
  }

}
