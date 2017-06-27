package org.pentaho.big.data.kettle.plugins.parquet.output;

import java.util.function.Function;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.StackLayout;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.pentaho.big.data.kettle.plugins.parquet.output.ParquetOutputDialog.FD;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.widget.TextVar;

public class PentahoFileChooser {

  private Group filenameGroup;
  
  private TextVar wPath;

  public PentahoFileChooser( PropsUI props, Composite parent, String blockTitle, VariableSpace variableSpace,
      Function<String, String> browseFileFunction ) {
    this( props, parent, null, blockTitle, variableSpace, browseFileFunction );
  }

  public PentahoFileChooser( PropsUI props, Composite parent, Control after, String blockTitle,
      VariableSpace variableSpace, Function<String, String> browseFileFunction ) {
    filenameGroup = new Group( parent, SWT.SHADOW_ETCHED_IN );
    filenameGroup.setText( blockTitle );
    filenameGroup.setLayout( new FormLayout() );
    props.setLook( filenameGroup );

    if ( after == null ) {
      new FD( filenameGroup ).left( 0, 0 ).right( 100, 0 ).top( 0, 0 ).apply();
    } else {
      new FD( filenameGroup ).left( 0, 0 ).right( 100, 0 ).top( after, 5 ).apply();
    }

    // stacked layout composite
    Composite stackedLayoutComposite = new Composite( filenameGroup, SWT.NONE );
    props.setLook( stackedLayoutComposite );

    StackLayout stackedLayout = new StackLayout();
    stackedLayoutComposite.setLayout( stackedLayout );

    Composite specifyFilenameComposite = new Composite( stackedLayoutComposite, SWT.NONE );
    Composite getFilenameFromFieldComposite = new Composite( stackedLayoutComposite, SWT.NONE );

    Button specifyFilenameButton = new Button( filenameGroup, SWT.RADIO );
    specifyFilenameButton.setText( "Specify filename" );
    specifyFilenameButton.setToolTipText( "Specify filename.Tooltip" );
    props.setLook( specifyFilenameButton );

    new FD( specifyFilenameButton ).left( 0, 10 ).top( 0, 10 ).apply();

    specifyFilenameButton.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        stackedLayout.topControl = specifyFilenameComposite;
        stackedLayoutComposite.layout();
      }
    } );

    Button getFilenameFromFieldButton = new Button( filenameGroup, SWT.RADIO );
    getFilenameFromFieldButton.setText( "Get filename from field" );
    getFilenameFromFieldButton.setToolTipText( "ExecRemote.Tooltip" );
    props.setLook( getFilenameFromFieldButton );

    new FD( getFilenameFromFieldButton ).left( 0, 10 ).top( specifyFilenameButton, 7 ).apply();

    getFilenameFromFieldButton.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        stackedLayout.topControl = getFilenameFromFieldComposite;
        stackedLayoutComposite.layout();
      }
    } );

    // separator
    Label environmentSeparator = new Label( filenameGroup, SWT.SEPARATOR | SWT.VERTICAL );
    new FD( environmentSeparator ).left( specifyFilenameButton, 50 ).bottom( 100, -10 ).top( 0, 10 ).apply();

    // FormData fd_environmentSeparator = new FormData();

    // fd_environmentSeparator.top = new FormAttachment( 0, 10 );
    // fd_environmentSeparator.left = new FormAttachment( wExecLocal, 50 );
    // fd_environmentSeparator.bottom = new FormAttachment( 100, -10 );
    // environmentSeparator.setLayoutData( fd_environmentSeparator );

    // stacked layout composite

    new FD( stackedLayoutComposite ).left( environmentSeparator, 5 ).bottom( 100, -10 ).top( 0, 0 ).right( 100, -7 )
        .apply();

    // FormData fd_stackedLayoutComposite = new FormData();
    // fd_stackedLayoutComposite.top = new FormAttachment( 0 );
    // fd_stackedLayoutComposite.left = new FormAttachment( environmentSeparator, 5 );
    // fd_stackedLayoutComposite.bottom = new FormAttachment( 100, -10 );
    // fd_stackedLayoutComposite.right = new FormAttachment( 100, -7 );
    // stackedLayoutComposite.setLayoutData( fd_stackedLayoutComposite );

    specifyFilenameComposite.setLayout( new FormLayout() );
    props.setLook( specifyFilenameComposite );

    Label wlPath = new Label( specifyFilenameComposite, SWT.LEFT );
    props.setLook( wlPath );
    wlPath.setText( "ParquetOutputDialog.Filename.Label" );
    new FD( wlPath ).left( 0, 0 ).right( 50, 0 ).top( 0, 10 ).apply();

    Button wbBrowse = new Button( specifyFilenameComposite, SWT.PUSH );
    props.setLook( wbBrowse );
    wbBrowse.setText( "System.Button.Browse" );
    new FD( wbBrowse ).top( wlPath, 5 ).right( 100, 0 ).apply();

    wPath = new TextVar( variableSpace, specifyFilenameComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPath );
    new FD( wPath ).left( 0, 0 ).top( wlPath, 5 ).right( wbBrowse, -10 ).apply();

    wbBrowse.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        String filePath = browseFileFunction.apply( wPath.getText() );
        if ( filePath != null ) {
          wPath.setText( filePath );
        }
      }
    } );

    getFilenameFromFieldComposite.setLayout( new FormLayout() );
    props.setLook( getFilenameFromFieldComposite );

    Label filenameFieldLabel = new Label( getFilenameFromFieldComposite, SWT.LEFT );
    props.setLook( filenameFieldLabel );
    filenameFieldLabel.setText( "Field" );
    new FD( filenameFieldLabel ).left( 0, 0 ).right( 50, 0 ).top( 0, 10 ).apply();

    CCombo filenameField = new CCombo( getFilenameFromFieldComposite, SWT.BORDER );
    props.setLook( filenameField );
    filenameField.add( "Field 1" );
    filenameField.add( "Field 2" );
    new FD( filenameField ).left( 0, 0 ).right( 50, 0 ).top( filenameFieldLabel, 5 ).apply();

    stackedLayout.topControl = specifyFilenameComposite;

    specifyFilenameButton.setSelection( true );
  }
  
  public Control getControl() {
    return filenameGroup;
  }

  public void setFilename( String fileName ) {
    wPath.setText( fileName );
  }

  public String getFilename() {
    return wPath.getText();
  }

}
