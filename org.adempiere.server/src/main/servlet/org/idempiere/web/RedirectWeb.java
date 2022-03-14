package org.idempiere.web;

import java.io.IOException;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class RedirectWeb extends HttpServlet {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6991848882036193856L;

	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
		resp.sendRedirect(req.getContextPath() + "/web/index.zul");
	}
}