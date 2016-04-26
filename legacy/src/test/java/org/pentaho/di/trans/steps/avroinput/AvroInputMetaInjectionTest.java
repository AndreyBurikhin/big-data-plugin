/*******************************************************************************
 *
 * Pentaho Big Data
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

package org.pentaho.di.trans.steps.avroinput;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.injection.BaseMetadataInjectionTest;

public class AvroInputMetaInjectionTest extends BaseMetadataInjectionTest<AvroInputMeta> {

  @Before
  public void setup() {
    setup( new AvroInputMeta() );
  }

  @Test
  public void test() throws Exception {
    check( "FILE_NAME", new StringGetter() {
      public String get() {
        return meta.getFilename();
      }
    } );
    check( "SCHEMA_FILE_NAME", new StringGetter() {
      public String get() {
        return meta.getSchemaFilename();
      }
    } );
    check( "AVRO_IN_FIELD", new BooleanGetter() {
      public boolean get() {
        return meta.getAvroInField();
      }
    } );
    check( "JSON_ENCODED", new BooleanGetter() {
      public boolean get() {
        return meta.getAvroIsJsonEncoded();
      }
    } );
    check( "FIELD_NAME", new StringGetter() {
      public String get() {
        return meta.getAvroFieldName();
      }
    } );
    check( "SCHEMA_IN_FIELD", new BooleanGetter() {
      public boolean get() {
        return meta.getSchemaInField();
      }
    } );
    check( "SCHEMA_FIELD_NAME", new StringGetter() {
      public String get() {
        return meta.getSchemaFieldName();
      }
    } );
    check( "SCHEMA_IN_FIELD_IS_PATH", new BooleanGetter() {
      public boolean get() {
        return meta.getSchemaInFieldIsPath();
      }
    } );
    check( "CACHE_SCHEMA_IN_MEMORY", new BooleanGetter() {
      public boolean get() {
        return meta.getCacheSchemasInMemory();
      }
    } );
    check( "DONT_COMPLAIN_ABOUT_MISSING_FIELDS", new BooleanGetter() {
      public boolean get() {
        return meta.getDontComplainAboutMissingFields();
      }
    } );

    check( "AVRO_FIELD_NAME", new StringGetter() {
      public String get() {
        return meta.getAvroFields().get( 0 ).m_fieldName;
      }
    } );
    check( "AVRO_FIELD_PATH", new StringGetter() {
      public String get() {
        return meta.getAvroFields().get( 0 ).m_fieldPath;
      }
    } );
    check( "AVRO_FIELD_TYPE", new StringGetter() {
      public String get() {
        return meta.getAvroFields().get( 0 ).m_kettleType;
      }
    } );

    check( "LOOKUP_FIELD_NAME", new StringGetter() {
      public String get() {
        return meta.getLookupFields().get( 0 ).m_fieldName;
      }
    } );
    check( "LOOKUP_VARIABLE_NAME", new StringGetter() {
      public String get() {
        return meta.getLookupFields().get( 0 ).m_variableName;
      }
    } );
    check( "LOOKUP_DEFAULT_VALUE", new StringGetter() {
      public String get() {
        return meta.getLookupFields().get( 0 ).m_defaultValue;
      }
    } );
  }

}
