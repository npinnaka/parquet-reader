package org.sf.pr;

import java.io.File;
import java.util.*;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javafx.application.Application;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.cell.MapValueFactory;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.Pane;
import javafx.scene.layout.VBox;
import javafx.stage.FileChooser;
import javafx.stage.Stage;

import static java.util.stream.Collectors.toList;


public class ParquetReader extends Application {

    private TableView table = new TableView();

    @Override
    public void start(final Stage stage) {
        stage.setTitle("File Chooser Sample");

        final FileChooser fileChooser = new FileChooser();
        final Button openMultipleButton = new Button("Load Parquet...");

        openMultipleButton.setOnAction(
                new EventHandler<ActionEvent>() {
                    @Override
                    public void handle(final ActionEvent e) {
                        configureFileChooser(fileChooser);
                        List<File> list =
                                fileChooser.showOpenMultipleDialog(stage);
                        List<String> names = list.stream().map(file -> file.getAbsolutePath()).collect(toList());
                        openFile(names);

                    }
                });


        final GridPane inputGridPane = new GridPane();

        GridPane.setConstraints(openMultipleButton, 1, 1);
        inputGridPane.setHgap(6);
        inputGridPane.setVgap(6);

// set scrollbars for cols
        table.setColumnResizePolicy(TableView.UNCONSTRAINED_RESIZE_POLICY);
        inputGridPane.getChildren().addAll(openMultipleButton, table);

        final Pane rootGroup = new VBox(12);
        rootGroup.getChildren().addAll(inputGridPane);
        rootGroup.setPadding(new Insets(12, 12, 12, 12));

        Scene scene = new Scene(rootGroup);
        stage.setScene(scene);

        stage.show();
    }


    private static void configureFileChooser(
            final FileChooser fileChooser) {
        fileChooser.setTitle("View files");
        fileChooser.setInitialDirectory(
                new File("/Users/pinnaka/IdeaProjects/clojure-spark/resources")
        );
        fileChooser.getExtensionFilters().addAll(
                new FileChooser.ExtensionFilter("All Files", "*.*"),
                new FileChooser.ExtensionFilter("Parquet", "*.jpg"),
                new FileChooser.ExtensionFilter("JSON", "*.json"),
                new FileChooser.ExtensionFilter("CSV", "*.csv")
        );
    }

    private void openFile(List<String> files) {
        try {
            processParquet(files);
        } catch (Exception ex) {
            Logger.getLogger(ParquetReader.class.getName()).log(
                    Level.SEVERE, null, ex
            );
        }
    }


    private void processParquet(List<String> files) {
        List<Map<String, Object>> sparkData = Utils.showDFData(Utils.readParquet(files));
        Map<String, Object> fetchKeysMap = sparkData.get(0);
        List<TableColumn> headers = new ArrayList<TableColumn>();
        for (String key : fetchKeysMap.keySet()) {
            TableColumn tableColumn = new TableColumn(key);
            tableColumn.setCellValueFactory(new MapValueFactory(key));
            headers.add(tableColumn);
        }
        table.getColumns().addAll(headers);
        table.setItems(getMapData(sparkData));
    }

    private ObservableList<Map> getMapData(List<Map<String, Object>> sparkData) {
        ObservableList<Map> mapData = FXCollections.observableArrayList();
        mapData.addAll(sparkData);
        return mapData;
    }

    public static void main(String[] args) {
        launch(args);
    }
}
