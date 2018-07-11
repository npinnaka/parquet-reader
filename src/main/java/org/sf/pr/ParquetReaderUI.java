package org.sf.pr;

import javafx.application.Application;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.geometry.HPos;
import javafx.geometry.Insets;
import javafx.geometry.Orientation;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.control.cell.MapValueFactory;
import javafx.scene.layout.*;
import javafx.stage.FileChooser;
import javafx.stage.Stage;

import java.io.File;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.util.stream.Collectors.toList;

public class ParquetReaderUI extends Application {

    private BorderPane root;
    private TableView table = null;
    private TextArea textArea = null;

    @Override
    public void start(Stage stage) {

        initUI(stage);
    }

    private void initUI(Stage stage) {

        root = new BorderPane();

        root.setTop(getTopPane(stage));
        root.setCenter(createCenterPane(stage, 0));

        Scene scene = new Scene(root);
        stage.setMaximized(true);
        stage.setTitle("Parquet Reader");
        stage.setScene(scene);
        stage.show();
    }


    private Node createCenterPane(Stage stage, int num) {
        GridPane gridPane = new GridPane();
        if (num == 0) {
            Label label = new Label("welcome to parquet utils");
            gridPane.setHalignment(label, HPos.CENTER);
            gridPane.getChildren().add(label);
        } else if (num == 1) {
            table = null;
            table = new TableView();
            gridPane.setHgrow(table, Priority.ALWAYS);
            gridPane.setVgrow(table, Priority.ALWAYS);
            gridPane.getChildren().add(table);
        } else if (num == 2) {
            textArea = new TextArea();
            gridPane.setHgrow(textArea, Priority.ALWAYS);
            gridPane.setVgrow(textArea, Priority.ALWAYS);
            gridPane.getChildren().add(textArea);
        }
        return gridPane;
    }

    private FlowPane getTopPane(Stage stage) {

        FlowPane flowPane = new FlowPane(Orientation.HORIZONTAL, 5, 5);
        root.setPadding(new Insets(10));


        final FileChooser parquetChooser = new FileChooser();
        final Button parquetOpenMultipleButton = new Button("Load Parquet...");

        parquetOpenMultipleButton.setOnAction(
                new EventHandler<ActionEvent>() {
                    @Override
                    public void handle(final ActionEvent e) {
                        root.setCenter(createCenterPane(stage, 1));
                        configureFileChooser(parquetChooser, "parquet");
                        List<File> list =
                                parquetChooser.showOpenMultipleDialog(stage);
                        List<String> names = list.stream().map(file -> file.getAbsolutePath()).collect(toList());
                        openFile(names, "parquet");

                    }
                });


        final FileChooser jsonChooser = new FileChooser();
        final Button jsonOpenMultipleButton = new Button("Load JSON...");

        jsonOpenMultipleButton.setOnAction(
                new EventHandler<ActionEvent>() {
                    @Override
                    public void handle(final ActionEvent e) {
                        root.setCenter(createCenterPane(stage, 1));
                        configureFileChooser(jsonChooser, "json");
                        File name =
                                jsonChooser.showOpenDialog(stage);
                        openFile(Arrays.asList(name.getAbsolutePath()), "json");
                    }
                });

        final Button createParquet = new Button("Create Parquet");

        createParquet.setOnAction(
                new EventHandler<ActionEvent>() {
                    @Override
                    public void handle(final ActionEvent e) {
                        root.setCenter(createCenterPane(stage, 2));
                    }
                });


        flowPane.getChildren().addAll(parquetOpenMultipleButton, jsonOpenMultipleButton, createParquet);
        return flowPane;
    }

    public static void main(String[] args) {
        launch(args);
    }

    private static void configureFileChooser(
            final FileChooser fileChooser, String fileType) {
        fileChooser.setTitle("View files");
        fileChooser.setInitialDirectory(
                new File("/Users/pinnaka/IdeaProjects/clojure-spark/resources")
        );
        Map<String, String> fileTypesMap = new HashMap<>();
        fileTypesMap.put("parquet", "*.parquet");
        fileTypesMap.put("json", "*.json;*.JSON");
        fileTypesMap.put("csv", "*.csv");

        fileChooser.getExtensionFilters().addAll(
                new FileChooser.ExtensionFilter(fileType, fileTypesMap.get(fileType))
        );
    }

    private void openFile(List<String> files, String fileType) {
        try {
            if ("parquet".equals(fileType))
            {
                processParquet(files);
            } else if ("json".equals(fileType)) {
                processJSON(files.get(0));
            }

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

    private void processJSON(String file) {
        List<Map<String, Object>> sparkData = Utils.showDFData(Utils.readJSON(file));
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
}