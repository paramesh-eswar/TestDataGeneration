package org.pgs.app;

import java.awt.Color;
import java.awt.Cursor;
import java.awt.EventQueue;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SwingConstants;
import javax.swing.border.EmptyBorder;

public class TestDataGeneratorUI extends JFrame {

	private static final long serialVersionUID = 1L;
	private JPanel contentPane;
	private JTextField numOfRowsTxtFld;
	private JTextField filenameTxtFld;
	private TestDataGeneratorV3 tdg = new TestDataGeneratorV3();

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
		contentPane.add(uploadFileBtn);
		
		JButton btnGenerateData = new JButton("Generate Data");
		btnGenerateData.setBounds(317, 169, 181, 44);
		btnGenerateData.setEnabled(false);
		contentPane.add(btnGenerateData);
		
		JTextArea resultPane = new JTextArea();
		resultPane.setEnabled(false);
		resultPane.setEditable(false);
		resultPane.setWrapStyleWord(true);
		resultPane.setLineWrap(true);
		resultPane.setFont(new Font("serif", Font.PLAIN, 14));
		
		JScrollPane scrollPane = new JScrollPane(resultPane, JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED, JScrollPane.HORIZONTAL_SCROLLBAR_AS_NEEDED);
		scrollPane.setBounds(50, 225, 679, 186);
		contentPane.add(scrollPane);
		
		uploadFileBtn.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent event) {
				if(event.getSource() == uploadFileBtn) {
					JFileChooser metadataFileUpload = new JFileChooser();
					int response = metadataFileUpload.showOpenDialog(null);
					if(response == JFileChooser.APPROVE_OPTION) {
						String filePath = metadataFileUpload.getSelectedFile().getAbsolutePath();
						filenameTxtFld.setText(filePath);
						resultPane.setText(null);
						btnGenerateData.setEnabled(true);
						tdg.errorMessage.setLength(0);
					}
				}
			}
		});
		
		btnGenerateData.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent event) {
				if(event.getSource() == btnGenerateData) {
					btnGenerateData.setEnabled(false);
					String filePath = filenameTxtFld.getText();
					Long numOfRows = 0l;
					try {
						numOfRows = Long.valueOf(numOfRowsTxtFld.getText());
						if(numOfRows<=0) {
							TestDataGeneratorUI.this.dispose();
							JOptionPane.showMessageDialog(null,"Number of rows should be whole number", "Invalid number of rows!", JOptionPane.INFORMATION_MESSAGE);
						} else {
							contentPane.setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
							try {
								// resultPane.setText("Test data generation is in progress ...\n");
								long startTime = System.currentTimeMillis();
								boolean isDataGenerated = tdg.generateTestData(filePath, numOfRows);
								long endTime = System.currentTimeMillis();
								resultPane.append(tdg.errorMessage.toString() + "\n");
							    if(isDataGenerated) {
							    	JOptionPane.showMessageDialog(null, tdg.errorMessage.toString() + "\nTime taken to generate test data: " + ((endTime-startTime)/1000) + " sec", "Data generated successfully!", JOptionPane.INFORMATION_MESSAGE);
							    	resultPane.append("Time taken to generate test data: " + ((endTime-startTime)/1000) + " sec");
							    }
							} catch (Exception e) {
								JOptionPane.showMessageDialog(null, tdg.errorMessage.toString(), "Error in Test Data Generation", JOptionPane.INFORMATION_MESSAGE);
								System.out.println(e);
							}
							contentPane.setCursor(Cursor.getDefaultCursor());
						}
					} catch (NumberFormatException nfe) {
						JOptionPane.showMessageDialog(null,"Number of rows should be whole number", "Invalid number of rows!", JOptionPane.INFORMATION_MESSAGE);
					}
				}
			}
		});
	}
}
