package org.idempiere.process;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.logging.Level;

import org.adempiere.base.IGridTabImporter;
import org.adempiere.base.equinox.EquinoxExtensionLocator;
import org.adempiere.exceptions.AdempiereException;
import org.compiere.model.MImportTemplate;
import org.compiere.model.MTable;
import org.compiere.model.Query;
import org.compiere.process.ProcessInfoParameter;
import org.compiere.process.SvrProcess;
import org.compiere.util.DB;
import org.compiere.util.Env;
import org.compiere.util.Util;

public class CreateColumnsFromCSV extends SvrProcess {
	
	private final static int AD_ImportTemplate_ID = new Query(Env.getCtx(), MImportTemplate.Table_Name, "Name='CreateColumns'", null).firstId();
	
	private InputStream m_file_istream = null;
	private MImportTemplate m_importTemplate;
	private String p_FileName = "";
	private String p_TableName = "";

	@Override
	protected void prepare() {
		for (ProcessInfoParameter para : getParameter()) {
			String name = para.getParameterName();
			if ("FileName".equals(name)) {
				p_FileName = para.getParameterAsString();
			} else {
				log.log(Level.SEVERE, "Unknown Parameter: " + name);
			}
		}
		p_TableName = MTable.getTableName(getCtx(), getRecord_ID());
	}

	@Override
	protected String doIt() throws Exception {
		if (!p_FileName.contains(p_TableName))
			return "表名和CSV文件不匹配，请确认文件正确";
		IGridTabImporter csvImport = initImporter();
		String ret = importFile (p_FileName, csvImport);
		return ret;
	}
	
	protected IGridTabImporter initImporter() throws Exception {
		IGridTabImporter csvImport = null;
		List<IGridTabImporter> importerList = EquinoxExtensionLocator.instance().list(IGridTabImporter.class).getExtensions();
		for (IGridTabImporter importer : importerList){
			if ("csv".equals(importer.getFileExtension())) {
				csvImport = importer;
				break;
			}
		}

		if (csvImport == null)
			throw new Exception ("No CSV importer");

		return csvImport;
	}
	
	protected String importFile(String filePath, IGridTabImporter csvImporter) throws Exception {
		try {
			m_file_istream = new FileInputStream(filePath);
			m_importTemplate = new MImportTemplate(getCtx(), AD_ImportTemplate_ID, get_TrxName());
	
			m_file_istream = m_importTemplate.validateFile(m_file_istream);
			
			BufferedReader reader = new BufferedReader(new InputStreamReader(m_file_istream));
			String line = null;
			boolean firstJumped = false;
			while ((line = reader.readLine()) != null) {
				if (!firstJumped) {
					firstJumped = true;
					continue;
				}
				String[] row = line.split(",");
				if (row.length != 7)
					return "导入项必须为7列";
				String columnName = row[0].strip().toLowerCase();
				if (checkColumnExists(columnName))
					continue;
				String columnType = row[1].strip().toLowerCase();
				String columnLength = row[2].strip().toLowerCase();
				String columnFrac = row[3].strip().toLowerCase();
				String columnNotNull = row[4].strip().toUpperCase();
				String columnDefaultValue = row[5].strip();
				String columnComment = row[6].strip();
				String realType = getColumnType(columnType, columnLength, columnFrac);
				String sql = generateAddColumnSQL(columnName, realType, columnNotNull, columnDefaultValue, columnComment);
				DB.executeUpdateEx(sql, get_TrxName());
			}
		} finally {
			m_file_istream.close();
		}
		return "导入成功";
	}

	private String generateAddColumnSQL(String name, String realType, String notNull, String defaultValue, String comment) {
		StringBuilder sql = new StringBuilder("ALTER TABLE adempiere.").append(p_TableName);
		sql.append(" ADD COLUMN ").append(name);
		sql.append(" ").append(realType).append(" ");
		if ("Y".equals(notNull))
			sql.append("NOT NULL ");
		if (!Util.isEmpty(defaultValue)) {
			sql.append("DEFAULT '").append(defaultValue).append("'");
		}
		sql.append(";");
		
		if (!Util.isEmpty(comment)) {
			sql.append("COMMENT ON COLUMN adempiere.").append(p_TableName).append(".").append(name);
			sql.append(" IS ");
			sql.append("'").append(comment).append("'");
			sql.append(";");
		}
		return sql.toString();
	}
	
	private String getColumnType(String type, String length, String frac) {
		if ("varchar".equals(type)) {
			return "varchar(" + length + ")";
		} else if ("numeric".equals(type)) {
			Integer len = Util.isEmpty(frac) ? 10 : Integer.parseInt(length);
			Integer f = Util.isEmpty(frac) ? 0 : Integer.parseInt(frac);
			if (f >= len)
				throw new AdempiereException("小数位数必须小于长度！");
			return "numeric(" + length + (f > 0 ? ", " + frac + ")" : ")");
		} else if ("timestamp".equals(type)) {
			return "timestamp(6)";
		}
		return type;
	}
	
	private boolean checkColumnExists(String name) {
		boolean exists = false;
		StringBuilder sql = new StringBuilder("SELECT 1 ");
		sql.append("FROM information_schema.columns ");
		sql.append("WHERE table_schema='adempiere' ");
		sql.append("AND table_name='").append(p_TableName).append("' AND column_name='").append(name).append("'");
		sql.append(";");
		PreparedStatement pstmt = null;
        ResultSet rs = null;
		try
		{
			pstmt = DB.prepareStatement(sql.toString(), get_TrxName());
			rs = pstmt.executeQuery();
			while (rs.next())
			{
				exists = true;
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
        finally
        {
            DB.close(rs, pstmt);
        }
		return exists;
	}
}
