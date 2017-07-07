/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2017 by Pentaho : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.formats.parquet.input;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.apache.commons.vfs2.FileObject;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.pentaho.big.data.kettle.plugins.formats.FormatInputField;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.trans.step.BaseFileStepDialog;
import org.pentaho.di.ui.util.SwtSvgImageUtil;
import org.pentaho.vfs.ui.CustomVfsUiPanel;
import org.pentaho.vfs.ui.VfsFileChooserDialog;

public class ParquetInputDialog extends BaseFileStepDialog<ParquetInputMetaBase> {

  private static final int DIALOG_WIDTH = 526;

  private static final int DIALOG_HEIGHT = 506;

  private static final int MARGIN = 15;

  private TableView wInputFields;

  private Label wlPath;

  private TextVar wPath;

  private Button wbBrowse;
  
  private VFSScheme selectedVFSScheme;

  public ParquetInputDialog( Shell parent, Object in, TransMeta transMeta, String sname ) {
    super( parent, (ParquetInputMetaBase) in, transMeta, sname );
  }

  @Override
  protected void createUI() {
    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = MARGIN;
    formLayout.marginHeight = MARGIN;

    shell.setSize( DIALOG_WIDTH, DIALOG_HEIGHT );
    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "ParquetInputDialog.Shell.Title" ) );

    lsOK = event -> ok();
    lsCancel = event -> cancel();

    Label wicon = new Label( shell, SWT.RIGHT );
    wicon.setImage( getImage() );
    props.setLook( wicon );
    new FD( wicon ).top( 0, 0 ).right( 100, 0 ).apply();

    // Stepname line
    wlStepname = new Label( shell, SWT.RIGHT );
    wlStepname.setText( BaseMessages.getString( PKG, "ParquetInputDialog.StepName.Label" ) );
    props.setLook( wlStepname );
    new FD( wlStepname ).left( 0, 0 ).top( 0, 0 ).apply();

    wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wStepname.setText( stepname );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    new FD( wStepname ).left( 0, 0 ).top( wlStepname, 5 ).width( 250 ).apply();

    Label spacer = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    new FD( spacer ).height( 1 ).left( 0, 0 ).top( wStepname, 15 ).right( 100, 0 ).apply();

    Label wlLocation = new Label( shell, SWT.RIGHT );
    wlLocation.setText( BaseMessages.getString( PKG, "ParquetInputDialog.Location.Label" ) );
    props.setLook( wlLocation );
    new FD( wlLocation ).left( 0, 0 ).top( spacer, MARGIN ).apply();

    CCombo wLocation = new CCombo( shell, SWT.BORDER );
    try {
      List<VFSScheme> availableVFSSchemes = getAvailableVFSSchemes();
      availableVFSSchemes.forEach( scheme -> wLocation.add( scheme.getSchemeName() ) );
      wLocation.addListener( SWT.Selection, event -> {
        this.selectedVFSScheme = availableVFSSchemes.get( wLocation.getSelectionIndex() );
      } );
      if ( !availableVFSSchemes.isEmpty() ) {
        wLocation.select( 0 );
        this.selectedVFSScheme = availableVFSSchemes.get( wLocation.getSelectionIndex() );
      }
    } catch ( Exception e ) {
      return;
    }
    props.setLook( wLocation );
    wLocation.addModifyListener( lsMod );
    new FD( wLocation ).left( 0, 0 ).top( wlLocation, 5 ).width( 150 ).apply();

    Label wlPath = new Label( shell, SWT.RIGHT );
    wlPath.setText( BaseMessages.getString( PKG, "ParquetInputDialog.Filename.Label" ) );
    props.setLook( wlPath );
    new FD( wlPath ).left( 0, 0 ).top( wLocation, 10 ).apply();

    wPath = new TextVar( transMeta, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPath );
    new FD( wPath ).left( 0, 0 ).top( wlPath, 5 ).width( 350 ).apply();

    wbBrowse = new Button( shell, SWT.PUSH );
    props.setLook( wbBrowse );
    wbBrowse.setText( BaseMessages.getString( PKG, "System.Button.Browse" ) );
    wbBrowse.addListener( SWT.Selection, event -> browseForFileOutputPath() );
    new FD( wbBrowse ).left( wPath, 5 ).top( wlPath, 5 ).apply();

    Label wlFields = new Label( shell, SWT.RIGHT );
    wlFields.setText( BaseMessages.getString( PKG, "ParquetInputDialog.Fields.Label" ) );
    props.setLook( wlFields );
    new FD( wlFields ).left( 0, 0 ).top( wPath, 10 ).apply();

    Button wGetFields = new Button( shell, SWT.PUSH );
    wGetFields.setText( BaseMessages.getString( PKG, "ParquetInputDialog.Fields.Get" ) );
    wGetFields.addListener( SWT.Selection, event -> {
      throw new RuntimeException( "Requires Shim API changes" );
    } );
    props.setLook( wGetFields );

    ColumnInfo[] parameterColumns =
        new ColumnInfo[] { new ColumnInfo( BaseMessages.getString( PKG, "ParquetInputDialog.Fields.column.AvroPath" ),
            ColumnInfo.COLUMN_TYPE_TEXT, false, false ), new ColumnInfo( BaseMessages.getString( PKG,
                "ParquetInputDialog.Fields.column.Name" ), ColumnInfo.COLUMN_TYPE_TEXT, false, false ), new ColumnInfo(
                    BaseMessages.getString( PKG, "ParquetInputDialog.Fields.column.Type" ),
                    ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaFactory.getValueMetaNames() ) };
    // parameterColumns[1].setUsingVariables( true );

    // JobExecutorParameters parameters = input.getParameters();
    wInputFields =
        new TableView( transMeta, shell, SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER, parameterColumns, 0, lsMod,
            props );
    props.setLook( wInputFields );

    // Label bottomSpacer = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    // new FD( bottomSpacer ).height( 1 ).left( 0, 0 ).bottom( wOK, 15 ).right( 100, 0 ).apply();

    // new FD( wComp ).left( 0, 0 ).top( 0, 0 ).right( 100, 0 ).bottom( 100, 0 ).apply();

    /*
     * CTabFolder wTabFolder = new CTabFolder( shell, SWT.BORDER ); props.setLook( wTabFolder, Props.WIDGET_STYLE_TAB );
     * wTabFolder.setSimple( false );
     */

    // addFilesTab( wTabFolder );
    // addFieldsTabs( wTabFolder );

    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );
    wCancel.addListener( SWT.Selection, lsCancel );
    new FD( wCancel ).right( 100, 0 ).bottom( 100, 0 ).apply();

    // Some buttons
    wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wOK.addListener( SWT.Selection, lsOK );
    new FD( wOK ).right( wCancel, -5 ).bottom( 100, 0 ).apply();

    wPreview = new Button( shell, SWT.PUSH );
    wPreview.setText( BaseMessages.getString( PKG, "ParquetInputDialog.Preview.Button" ) );
    wPreview.addListener( SWT.Selection, event -> {
      throw new RuntimeException( "Requires Shim API changes" );
    } );
    new FD( wPreview ).right( wOK, -50 ).bottom( 100, 0 ).apply();

    Label hSpacer = new Label( shell, SWT.HORIZONTAL | SWT.SEPARATOR );
    new FD( hSpacer ).height( 1 ).left( 0, 0 ).bottom( wCancel, -15 ).right( 100, 0 ).apply();

    new FD( wGetFields ).bottom( hSpacer, -15 ).right( 100, 0 ).apply();
    new FD( wInputFields ).left( 0, 0 ).right( 100, 0 ).top( wlFields, 5 ).bottom( wGetFields, -10 ).apply();
    // new FD( wTabFolder ).left( 0, 0 ).top( spacer, 15 ).right( 100, 0 ).bottom( hSpacer, -15 ).apply();
    // wTabFolder.setSelection( 0 );

  }

  /*
   * private void addFilesTab( CTabFolder wTabFolder ) { CTabItem wTab = new CTabItem( wTabFolder, SWT.NONE );
   * wTab.setText( BaseMessages.getString( PKG, "ParquetInputDialog.FileTab.TabTitle" ) );
   * 
   * ScrolledComposite wSComp = new ScrolledComposite( wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL ); wSComp.setLayout( new
   * FillLayout() );
   * 
   * Composite wComp = new Composite( wSComp, SWT.NONE ); props.setLook( wComp );
   * 
   * FormLayout layout = new FormLayout(); layout.marginWidth = 15; layout.marginHeight = 15; wComp.setLayout( layout );
   * 
   * wlPath = new Label( wComp, SWT.LEFT ); props.setLook( wlPath ); wlPath.setText( BaseMessages.getString( PKG,
   * "ParquetInputDialog.Filename.Label" ) ); new FD( wlPath ).left( 0, 0 ).right( 50, 0 ).top( 0, 0 ).apply();
   * 
   * wbBrowse = new Button( wComp, SWT.PUSH ); props.setLook( wbBrowse ); wbBrowse.setText( BaseMessages.getString( PKG,
   * "System.Button.Browse" ) ); new FD( wbBrowse ).top( wlPath, 5 ).right( 100, 0 ).apply();
   * 
   * wPath = new TextVar( transMeta, wComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER ); props.setLook( wPath ); new FD( wPath
   * ).left( 0, 0 ).top( wlPath, 5 ).right( wbBrowse, -10 ).apply();
   * 
   * new FD( wComp ).left( 0, 0 ).top( 0, 0 ).right( 100, 0 ).bottom( 100, 0 ).apply(); wComp.pack();
   * 
   * Rectangle bounds = wComp.getBounds(); wSComp.setContent( wComp ); wSComp.setExpandHorizontal( true );
   * wSComp.setExpandVertical( true ); wSComp.setMinWidth( bounds.width ); wSComp.setMinHeight( bounds.height );
   * 
   * wTab.setControl( wSComp );
   * 
   * wbBrowse.addSelectionListener( new SelectionAdapter() { public void widgetSelected( SelectionEvent e ) {
   * DirectoryDialog dialog = new DirectoryDialog( shell, SWT.OPEN ); if ( wPath.getText() != null ) { String fpath =
   * transMeta.environmentSubstitute( wPath.getText() ); dialog.setFilterPath( fpath ); }
   * 
   * if ( dialog.open() != null ) { String str = dialog.getFilterPath(); wPath.setText( str ); } } } ); }
   */

  /*
   * private void addFieldsTabs( CTabFolder wTabFolder ) { CTabItem wTab = new CTabItem( wTabFolder, SWT.NONE );
   * wTab.setText( BaseMessages.getString( PKG, "ParquetInputDialog.FieldsTab.TabTitle" ) );
   * 
   * ScrolledComposite wSComp = new ScrolledComposite( wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL ); wSComp.setLayout( new
   * FillLayout() );
   * 
   * Composite wComp = new Composite( wSComp, SWT.NONE ); props.setLook( wComp );
   * 
   * FormLayout layout = new FormLayout(); layout.marginWidth = 15; layout.marginHeight = 15; wComp.setLayout( layout );
   * 
   * Button wIgnore = new Button( wComp, SWT.CHECK ); wIgnore.setText( BaseMessages.getString( PKG,
   * "ParquetInputDialog.Fields.Ignore" ) ); props.setLook( wIgnore ); new FD( wIgnore ).left( 0, 0 ).top( 0, 0
   * ).apply(); // wIgnore.setSelection( jobExecutorMeta.getParameters().isInheritingAllVariables() );
   * 
   * Button wGetFields = new Button( wComp, SWT.PUSH ); wGetFields.setText( BaseMessages.getString( PKG,
   * "ParquetInputDialog.Fields.Get" ) ); props.setLook( wGetFields ); new FD( wGetFields ).bottom( 100, 0 ).right( 100,
   * 0 ).apply(); ; // wGetParameters.setSelection( jobExecutorMeta.getParameters().isInheritingAllVariables() ); //
   * wGetParameters.addSelectionListener( new SelectionAdapter() { // public void widgetSelected( SelectionEvent e ) {
   * // getParametersFromJob( null ); // null : reload file // } // } );
   * 
   * ColumnInfo[] parameterColumns = new ColumnInfo[] { new ColumnInfo( BaseMessages.getString( PKG,
   * "ParquetInputDialog.Fields.column.Name" ), ColumnInfo.COLUMN_TYPE_TEXT, false, false ), new ColumnInfo(
   * BaseMessages.getString( PKG, "ParquetInputDialog.Fields.column.Path" ), ColumnInfo.COLUMN_TYPE_CCOMBO, new String[]
   * {}, false ), new ColumnInfo( BaseMessages.getString( PKG, "ParquetInputDialog.Fields.column.Type" ),
   * ColumnInfo.COLUMN_TYPE_TEXT, false, false ), new ColumnInfo( BaseMessages.getString( PKG,
   * "ParquetInputDialog.Fields.column.Indexed" ), ColumnInfo.COLUMN_TYPE_TEXT, false, false ), };
   * parameterColumns[1].setUsingVariables( true );
   * 
   * // JobExecutorParameters parameters = input.getParameters(); wInputFields = new TableView( transMeta, wComp,
   * SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER, parameterColumns, 3, lsMod, props ); props.setLook( wInputFields );
   * new FD( wInputFields ).left( 0, 0 ).right( 100, 0 ).top( wIgnore, 10 ).bottom( wGetFields, -10 ).apply();
   * 
   * // for ( int i = 0; i < parameters.getVariable().length; i++ ) { // TableItem tableItem =
   * wJobExecutorParameters.table.getItem( i ); // tableItem.setText( 1, Const.NVL( parameters.getVariable()[ i ], "" )
   * ); // tableItem.setText( 2, Const.NVL( parameters.getField()[ i ], "" ) ); // tableItem.setText( 3, Const.NVL(
   * parameters.getInput()[ i ], "" ) ); // } wInputFields.setRowNums(); wInputFields.optWidth( true );
   * 
   * new FD( wComp ).left( 0, 0 ).top( 0, 0 ).right( 100, 0 ).bottom( 100, 0 ).apply(); wComp.pack();
   * 
   * Rectangle bounds = wComp.getBounds(); wSComp.setContent( wComp ); wSComp.setExpandHorizontal( true );
   * wSComp.setExpandVertical( true ); wSComp.setMinWidth( bounds.width ); wSComp.setMinHeight( bounds.height );
   * 
   * wTab.setControl( wSComp );
   * 
   * }
   */

  protected void browseForFileOutputPath() {
    try {
      
      VfsFileChooserDialog fileChooserDialog = getVfsFileChooserDialog();
//      FileObject selectedFile =
//          fileChooserDialog.open( shell, null, HadoopSpoonPlugin.HDFS_SCHEME, true, null, fileFilters,
//          fileFilterNames, VfsFileChooserDialog.VFS_DIALOG_OPEN_FILE_OR_DIRECTORY );
    } catch ( KettleException e ) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
  }

  List<VFSScheme> getAvailableVFSSchemes() throws KettleException {
    VfsFileChooserDialog fileChooserDialog = getVfsFileChooserDialog();
    List<CustomVfsUiPanel> customVfsUiPanels = fileChooserDialog.getCustomVfsUiPanels();
    List<VFSScheme> vfsSchemes = new ArrayList<>();
    customVfsUiPanels.forEach( vfsPanel -> {
      VFSScheme scheme = new VFSScheme( vfsPanel.getVfsScheme(), vfsPanel.getVfsSchemeDisplayText() );
      vfsSchemes.add( scheme );
    } );
    return vfsSchemes;
  }

  VfsFileChooserDialog getVfsFileChooserDialog() throws KettleException {
    try {
      FileObject initialFile = KettleVFS.getFileObject( getSpoon().getLastFileOpened() );
      FileObject rootFile = initialFile.getFileSystem().getRoot();
      return getSpoon().getVfsFileChooserDialog( rootFile, initialFile );
    } catch ( Exception e ) {
      //TODO fix exception message
      throw new KettleException( e );
    }
  }

  Spoon getSpoon() {
    return Spoon.getInstance();
  }

  protected Image getImage() {
    return SwtSvgImageUtil.getImage( shell.getDisplay(), getClass().getClassLoader(), "PI.svg", ConstUI.ICON_SIZE,
        ConstUI.ICON_SIZE );
  }

  /**
   * Read the data from the meta object and show it in this dialog.
   */
  @Override
  protected void getData( ParquetInputMetaBase meta ) {
    if ( meta.dir != null ) {
      wPath.setText( meta.dir );
    }

    int nrFields = meta.inputFields.length;
    for ( int i = 0; i < nrFields; i++ ) {
      FormatInputField outputField = meta.inputFields[i];
      TableItem item = wInputFields.table.getItem( i );
      if ( outputField.getName() != null ) {
        item.setText( 1, outputField.getName() );
      }
      item.setText( 3, outputField.getTypeDesc() );
    }
  }

  /**
   * Fill meta object from UI options.
   */
  @Override
  protected void getInfo( ParquetInputMetaBase meta, boolean preview ) {
    meta.dir = wPath.getText();
    int nrFields = wInputFields.nrNonEmpty();

    FormatInputField[] inputFields = new FormatInputField[nrFields];
    for ( int i = 0; i < nrFields; i++ ) {
      TableItem item = wInputFields.getNonEmpty( i );

      inputFields[i] = new FormatInputField();
      inputFields[i].setName( item.getText( 1 ) );
      inputFields[i].setType( item.getText( 3 ) );
    }
    meta.inputFields = inputFields;
  }
}
