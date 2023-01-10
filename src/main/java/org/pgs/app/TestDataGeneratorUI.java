package org.pgs.app;

import java.awt.EventQueue;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.border.Border;
import javax.swing.border.EmptyBorder;
import javax.swing.JLabel;
import java.awt.Font;
import java.awt.LayoutManager;
import java.awt.LayoutManager2;

import javax.swing.SwingConstants;
import javax.swing.JTextField;
import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JFileChooser;

import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;
import javax.swing.JTextPane;
import java.awt.Color;

public class TestDataGeneratorUI extends JFrame {

	private static final long serialVersionUID = 1L;
	private JPanel contentPane;
	private JTextField numOfRowsTxtFld;
	private JTextField filenameTxtFld;

	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					TestDataGeneratorUI frame = new TestDataGeneratorUI();
					frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * Create the frame.
	 */
	public TestDataGeneratorUI() {
		setTitle("Test Data Generator Tool - v3");
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 793, 469);
		contentPane = new JPanel();
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));

		setContentPane(contentPane);
		contentPane.setLayout(null);
		contentPane.setBackground(Color.LIGHT_GRAY);
		
		JLabel tdgHeadingLbl = new JLabel("Test Data Generator");
		tdgHeadingLbl.setBounds(282, 31, 211, 15);
		tdgHeadingLbl.setHorizontalAlignment(SwingConstants.CENTER);
		tdgHeadingLbl.setFont(new Font("serif", Font.PLAIN, 18));
		contentPane.add(tdgHeadingLbl);
		
		JLabel numberOfRowsLbl = new JLabel("Number of rows :");
		numberOfRowsLbl.setBounds(75, 88, 133, 23);
		numberOfRowsLbl.setFont(new Font("serif", Font.PLAIN, 14));
		contentPane.add(numberOfRowsLbl);
		
		numOfRowsTxtFld = new JTextField();
		numOfRowsTxtFld.setBounds(209, 86, 183, 27);
		contentPane.add(numOfRowsTxtFld);
		numOfRowsTxtFld.setColumns(10);
		
		JLabel metadataFileLbl = new JLabel("Metadata File :");
		metadataFileLbl.setBounds(75, 132, 122, 23);
		metadataFileLbl.setFont(new Font("serif", Font.PLAIN, 14));
		contentPane.add(metadataFileLbl);
		
		filenameTxtFld = new JTextField();
		filenameTxtFld.setBounds(209, 130, 341, 27);
		filenameTxtFld.setEnabled(false);
		filenameTxtFld.setEditable(false);
		filenameTxtFld.setFont(new Font("serif", Font.BOLD, 14));
		contentPane.add(filenameTxtFld);
		
		JButton uploadFileBtn = new JButton("Upload File");
		uploadFileBtn.setBounds(574, 126, 133, 34);
		uploadFileBtn.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent event) {
				if(event.getSource() == uploadFileBtn) {
					JFileChooser metadataFileUpload = new JFileChooser();
					int response = metadataFileUpload.showOpenDialog(null);
					if(response == JFileChooser.APPROVE_OPTION) {
						String filePath = metadataFileUpload.getSelectedFile().getAbsolutePath();
						filenameTxtFld.setText(filePath);
					}
				}
			}
		});
		contentPane.add(uploadFileBtn);
		
		JTextPane resultPane = new JTextPane();
		resultPane.setEnabled(false);
		resultPane.setEditable(false);
		resultPane.setBounds(50, 225, 679, 186);
		contentPane.add(resultPane);
		
		JButton btnGenerateData = new JButton("Generate Data");
		btnGenerateData.setBounds(317, 169, 181, 44);
		btnGenerateData.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent event) {
				if(event.getSource() == btnGenerateData) {
					String filePath = filenameTxtFld.getText();
					Long numOfRows = Long.valueOf(numOfRowsTxtFld.getText());
					long startTime = System.currentTimeMillis();
					TestDataGeneratorV3 tdg = new TestDataGeneratorV3();
					boolean isDataGenerated = tdg.generateTestData(filePath, numOfRows);
					long endTime = System.currentTimeMillis();
					if(isDataGenerated)
						resultPane.setText("Time taken to generate test data: " + ((endTime-startTime)/1000) + " sec");
					else
						resultPane.setText("Test data genearation failed with errors!!");
				}
			}
		});
		contentPane.add(btnGenerateData);
	}
}
