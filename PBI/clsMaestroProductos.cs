using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Npgsql; //Npgsql .NET Data Provider for PostgreSQL

using System.Net.Mail;
using System.Configuration;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Net.Security;
using Outlook = Microsoft.Office.Interop.Outlook;    

namespace ETL.PBI
{
    class clsMaestroProductos
    {

        public static void registrar_datos(string cadena)
        {
            StreamWriter fichero;
            fichero = File.AppendText("reporte.txt");
            fichero.WriteLine(cadena);
            fichero.Close();
            return;
        }

        public static void registrar_error(string pEmpresa, int pCapa, string pRutina, string pDetalle)
        {
            string err = "";
            try
            {

                string fec_Hr = DateTime.Now.ToString() + " - " + DateTime.Now.ToString("hh:mm:ss tt");
                pDetalle = fec_Hr + " * " + pDetalle;

                NpgsqlConnection conn = new NpgsqlConnection(ETL.Program._cn);
                string lsql;
                lsql = "'" + pEmpresa + "'," +
                        "" + pCapa + "," +
                        "'" + pRutina + "'," +
                        "'" + pDetalle + "'";

                NpgsqlConnection.ClearAllPools();
                conn.Open();
                
                string filename = "errores.txt";
                string cadena="registrar_error ==>  (" + lsql + ")";
                //registrar_datos(filename);

                /*
                NpgsqlCommand cmd = new NpgsqlCommand("select public.fx_registra_error_carga (" + lsql + ")", conn);
                NpgsqlDataReader dr = cmd.ExecuteReader();
                dr.Close();
                conn.Close();
                */

                NpgsqlConnection.ClearAllPools();
            }
            catch (Exception e)
            {
                err = e.Message;
                NpgsqlConnection.ClearAllPools();
            }
        }

        public static async Task<string> correo()
        {
            string err = "";
            try
             {

                var outlookApp = new Outlook.Application();
                var mailItem = (Outlook.MailItem)outlookApp.CreateItem(Outlook.OlItemType.olMailItem);

                int fila = 1;
                string lsCantidad;
                string lsSubject = "NUEVA ACTUALIZACIÓN DE BASE DE DATOS ESPEJO - " + DateTime.Now.ToString("hh:mm:ss");
                string lsHtml = "<html><head></head><body><Table style='width: 800px;'><tr><td>Estimados, <br>Acaba de finalizar la actualización de la base de datos espejo.<br></td></tr></table><br>" +
                "<Table style='width: 800px' border=1><tr bgcolor='02361A'align='center'><td><b> N°</b></td><td><b>Tablas</b></td><td><b>Fecha</b></td><td><b>H.Inicio</b></td><td> <b>H.Fin</b></td><td><b>Cantidad</b></td></tr>";

                NpgsqlConnection conn = new NpgsqlConnection(ETL.Program._cn);
                NpgsqlConnection.ClearAllPools();
                conn.Open();

                await using (NpgsqlCommand cmd = new NpgsqlCommand("select * from actualizaciones order by fecha, hora_ini", conn))
                {
                    await using (NpgsqlDataReader dr = await cmd.ExecuteReaderAsync())
                        while (await dr.ReadAsync())
                        {
                            lsCantidad = Convert.ToInt32(dr[4]).ToString("N0");
                            lsHtml = lsHtml + "<tr border=1><td>" + fila + "</td><td>" + Convert.ToString(dr[0]) + "</td><td align='center'>" + Convert.ToString(dr[1]).Substring(0, 10) + "</td><td align='center'>" + Convert.ToString(dr[2]) + "</td><td align='center'>" + Convert.ToString(dr[3]) + "</td><td align='right'> " + lsCantidad + "</td></tr>";
                            fila = fila + 1;
                        }
                }
                conn.Close();
                NpgsqlConnection.ClearAllPools();
                lsHtml = lsHtml + "</table><br><Table style='width: 800px;'><tr><td> <br><br> Saludos <br> Carlos Cáceres </td></tr></table></body></html>";

                mailItem.HTMLBody = lsHtml; 
                mailItem.Subject = lsSubject;
                //mailItem.Body= lsHtml;
                mailItem.To = "carlos.caceres@vielco.com;";
                mailItem.Send();
                System.Runtime.InteropServices.Marshal.FinalReleaseComObject(mailItem);
                System.Runtime.InteropServices.Marshal.FinalReleaseComObject(outlookApp);
                return "1";
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error("TODAS", 2, "correo", e.Message);
                Console.WriteLine("ERROR -->" + err);
                return "1";
            }
        }

        public static async Task<string> correo2()
        {
            string err = "";
            try
           {
                int fila = 1;
                string lsCantidad;
                string lsSubject = "ACTUALIZACIÓN DE B.D DE POWER BI - " + DateTime.Now.ToString("hh:mm:ss");
                string lsHtml = "<html><head></head><body><Table style='width: 800px;'><tr><td> Estimados, <br> Acaba de rfinalizar la actualización de la base de datos de power BI desde SAP.<br></td></tr></table><br>" +
                    "<Table style='width: 800px' border=1><tr bgcolor='02361A'align='center'><td><b> N°</b></td><td><b>Tablas</b></td><td><b>Fecha</b></td><td><b>H.Inicio</b></td><td> <b>H.Fin</b></td><td><b>Cantidad</b></td></tr>";

                NpgsqlConnection conn = new NpgsqlConnection(ETL.Program._cn);
                NpgsqlConnection.ClearAllPools();
                conn.Open();

                await using (NpgsqlCommand cmd = new NpgsqlCommand("select * from actualizaciones order by hora_ini", conn))
                {
                    await using (NpgsqlDataReader dr = await cmd.ExecuteReaderAsync())
                        while (await dr.ReadAsync())
                        {
                            //lsCantidad = Convert.ToInt32(dr[4]);
                            lsCantidad = Convert.ToInt32(dr[4]).ToString("N0");

                            lsHtml = lsHtml + "<tr border=1><td>" + fila + "</td><td>" + Convert.ToString(dr[0]) + "</td><td align='center'>" + Convert.ToString(dr[1]).Substring(0, 10) + "</td><td align='center'>" + Convert.ToString(dr[2]) + "</td><td align='center'>" + Convert.ToString(dr[3]) + "</td><td align='right'> " + lsCantidad + "</td></tr>";
                            fila = fila + 1;
                        }
                }
                conn.Close();
                NpgsqlConnection.ClearAllPools();
                lsHtml = lsHtml + "</table><br><Table style='width: 800px;'><tr><td> <br><br> Saludos <br> Carlos Cáceres </td></tr></table></body></html>";

                MailMessage correo = new MailMessage();
                correo.From = new MailAddress("jefe.tecnologia@vielco.com", "Carlos Cáceres ", System.Text.Encoding.UTF8);//Correo de salida
                //correo.To.Add("ccacerva@gmail.com,carlos.caceres@vielco.com"); //Correo destino?
                correo.To.Add("carlos.caceres@vielco.com"); //Correo destino?
                correo.Subject = lsSubject;
                correo.Body = lsHtml;
                correo.IsBodyHtml = true;
                correo.Priority = MailPriority.Normal;
                SmtpClient smtp = new SmtpClient();
                smtp.UseDefaultCredentials = false;
                smtp.Host = " smtp.office365.com";
                smtp.Port = 587;
                smtp.Credentials = new System.Net.NetworkCredential("jefe.tecnologia@vielco.com", "CAcv01__");//Cuenta de correo
                ServicePointManager.ServerCertificateValidationCallback = delegate (object s, X509Certificate certificate, X509Chain chain, SslPolicyErrors sslPolicyErrors) { return true; };
                smtp.EnableSsl = true;//True si el servidor de correo permite ssl
                smtp.Send(correo);
                return "1";
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error("TODAS", 2, "correo", e.Message);
                Console.WriteLine("ERROR -->" + err);
                return "1";
            }
        }

        public static string CONTROL_CARGAS_ESPEJO(int pID, int pAccion)
        {
            string err = "";
            try
            {
                NpgsqlConnection conn = new NpgsqlConnection(ETL.Program._cn);
                NpgsqlConnection.ClearAllPools();
                conn.Open();
                NpgsqlCommand cmd = new NpgsqlCommand("CALL public.spf_control_cargas_espejo(" + pID + "," + pAccion + ")", conn);//pID=Tabla  //  pAccion= Sobre TMP o PRD
                NpgsqlDataReader dr = cmd.ExecuteReader();
                dr.Close();
                conn.Close();
                NpgsqlConnection.ClearAllPools();
                return "OK";
            }
            catch (Exception e)
            {
                err = e.Message;
                NpgsqlConnection.ClearAllPools();
                registrar_error("TODAS", 2, "iniciarCARGA", e.Message);
                return err;
            }
        }

        public static async Task<string> obtener_rango_fechas()
        {
            string err = "";
            string mje = "";
            try
            {
                NpgsqlConnection conn = new NpgsqlConnection(ETL.Program._cn);
                NpgsqlConnection.ClearAllPools();
                conn.Open();

                await using (NpgsqlCommand cmd = new NpgsqlCommand("SELECT id,prodedimiento, f_desde, f_hasta from zconf_rango_fechas order by id", conn))
                {
                    await using (NpgsqlDataReader dr = await cmd.ExecuteReaderAsync())
                        while (await dr.ReadAsync())
                        {
                            if (Convert.ToString(dr[0]) == "1")//LIBRO MAYOR  9- 1
                            {
                                ETL.Program.FD8 = Convert.ToString(dr[2]).Substring(6, 4) + Convert.ToString(dr[2]).Substring(3, 2) + Convert.ToString(dr[2]).Substring(0, 2);
                                ETL.Program.FH8 = Convert.ToString(dr[3]).Substring(6, 4) + Convert.ToString(dr[3]).Substring(3, 2) + Convert.ToString(dr[3]).Substring(0, 2);
                            }

                            if (Convert.ToString(dr[0]) == "2")//VENTAS  8- 2
                            {
                                ETL.Program.FD22 = Convert.ToString(dr[2]).Substring(6, 4) + Convert.ToString(dr[2]).Substring(3, 2) + Convert.ToString(dr[2]).Substring(0, 2);
                                ETL.Program.FH22 = Convert.ToString(dr[3]).Substring(6, 4) + Convert.ToString(dr[3]).Substring(3, 2) + Convert.ToString(dr[3]).Substring(0, 2);
                                ETL.Program.FII_F = Convert.ToDateTime(dr[2]);
                                ETL.Program.FFF_F = Convert.ToDateTime(dr[3]);
                                ETL.Program.FII_FResp = ETL.Program.FII_F;
                                ETL.Program.FFF_FResp = ETL.Program.FFF_F;

                            }

                            if (Convert.ToString(dr[0]) == "3")//ZSPQ_SSTT_ReporteOT  6-3
                            {
                                ETL.Program.FD6 = Convert.ToString(dr[2]).Substring(6, 4) + Convert.ToString(dr[2]).Substring(3, 2) + Convert.ToString(dr[2]).Substring(0, 2);
                                ETL.Program.FH6 = Convert.ToString(dr[3]).Substring(6, 4) + Convert.ToString(dr[3]).Substring(3, 2) + Convert.ToString(dr[3]).Substring(0, 2);
                            }

                            if (Convert.ToString(dr[0]) == "4")//ZSPQ_NotaVtasDigitadas 7 -4 
                            {
                                ETL.Program.FD7 = Convert.ToString(dr[2]).Substring(6, 4) + Convert.ToString(dr[2]).Substring(3, 2) + Convert.ToString(dr[2]).Substring(0, 2);
                                ETL.Program.FH7 = Convert.ToString(dr[3]).Substring(6, 4) + Convert.ToString(dr[3]).Substring(3, 2) + Convert.ToString(dr[3]).Substring(0, 2);
                            }
                            //dr.Close();
                        }

                }
                conn.Close();
                NpgsqlConnection.ClearAllPools();
                return mje;
            }
            catch (Exception e)
            {
                err = e.Message;
                NpgsqlConnection.ClearAllPools();
                registrar_error("TODAS", 2, "obtener_rango_fechas", e.Message);
                return err;
            }
        }


        public static async Task<string> carga_data_update(string pTabla, int registros, DateTime pFEcha, string pH_Ini, string pH_Fin)
        {
            string err = "";
            string mje = "";

            try
            {
                NpgsqlConnection conn = new NpgsqlConnection(ETL.Program._cn);
                NpgsqlConnection.ClearAllPools();
                conn.Open();

                string lsql;
                lsql = "'" + pTabla + "'," +
                        "'" + pFEcha + "'," +
                        "'" + pH_Ini + "'," +
                        "'" + pH_Fin + "'," +
                        "" + registros + "";

                /*StreamWriter fichero;
                fichero = File.AppendText("C:\\Users\\carlos.caceres\\OneDrive - Grupo Vielva\\Escritorio\\Nueva carpeta (8)\\data.txt");
                fichero.WriteLine("select public.fx_carga_stock_productos (" + lsql + ")");
                fichero.Close();*/


                await using (NpgsqlCommand cmd = new NpgsqlCommand("select public.fx_carga_actualizaciones (" + lsql + ")", conn))
                {
                    await using (NpgsqlDataReader dr = await cmd.ExecuteReaderAsync())
                        while (await dr.ReadAsync())
                        {
                            mje = Convert.ToString(dr[0]);
                            ETL.Program.myContador = ETL.Program.myContador + 1;
                            //dr.Close();
                        }
                }
                conn.Close();
                NpgsqlConnection.ClearAllPools();
                return mje;
            }
            catch (Exception e)
            {
                err = e.Message;
                NpgsqlConnection.ClearAllPools();
                registrar_error("TODAS", 2, "carga_data_update", e.Message);
                return err;
            }
        }



        public static async Task<string> ingresa_ERROR_carga(string pEmpresa, int pCapa, string pRutina, string pDetalle)
        {
            string err = "";
            string mje = "";
            try
            {
                NpgsqlConnection conn = new NpgsqlConnection(ETL.Program._cn);
                string lsql;
                lsql =  "'" + pEmpresa + "'," +
                        "" + pCapa + "," +
                        "'" + pRutina + "'," +
                        "'" + pDetalle + "'";

                /*
                StreamWriter fichero;
                fichero = File.AppendText("C:\\Users\\carlos.caceres\\OneDrive - Grupo Vielva\\Escritorio\\Nueva carpeta (8)\\data.txt");
                fichero.WriteLine("select public.fx_carga_detalle_venta_artic (" + lsql + ")");
                fichero.Close();
                */
                NpgsqlConnection.ClearAllPools();
                conn.Open();
                await using (NpgsqlCommand cmd = new NpgsqlCommand("select public.fx_registra_error_carga (" + lsql + ")", conn))
                {
                    await using (NpgsqlDataReader dr = await cmd.ExecuteReaderAsync())
                        while (await dr.ReadAsync())
                        {
                            mje = Convert.ToString(dr[0]);
                            ETL.Program.myContador = ETL.Program.myContador + 1;
                            //dr.Close();
                        }
                }
                conn.Close();
                NpgsqlConnection.ClearAllPools();
                return mje;

            }
            catch (Exception e)
            {
                err = e.Message;
                return err;
            }
        }


        public static async Task<string> carga_libro_mayorV2()
        {
            string err = "";
            string mje = "";
            string pEmpresa = "";
            try
            {
                //mje = ETL.Program.LibroMayorALL[1].Country;
                string lspBaseRef = "";
                string lspFolio = "";
                string lspLineMemo = "";
                string lspFormatCode = "";
                string lspAcctName = "";
                string ls_pPriceBefDi = "";
                string lspOcrCode1 = ""; 
                string lspOcrCode2 = "";
                string lspOcrCode3 = "";
                string lspContraAct = "";
                string lspRefDate = "";
                string lsCountry = "";
                string lspTransType = "";
                string lspContraActName = "";
                
                string lsql;
                NpgsqlConnection conn = new NpgsqlConnection(ETL.Program._cn);
                conn.Open();

                for (int i = 0; i < ETL.Program.LibroMayorALL.Count; i++)
                    {
                    lspBaseRef = ""; if (ETL.Program.LibroMayorALL[i].BaseRef is not null) lspBaseRef = Convert.ToString(ETL.Program.LibroMayorALL[i].BaseRef).Replace("\'", " ");
                    lspFolio = ""; if (ETL.Program.LibroMayorALL[i].Folio is not null) lspFolio = Convert.ToString(ETL.Program.LibroMayorALL[i].Folio).Replace("\'", " ");
                    lspLineMemo = ""; if (ETL.Program.LibroMayorALL[i].LineMemo is not null) lspLineMemo = Convert.ToString(ETL.Program.LibroMayorALL[i].LineMemo).Replace("\'", " ");
                    lspFormatCode = ""; if (ETL.Program.LibroMayorALL[i].FormatCode is not null) lspFormatCode = Convert.ToString(ETL.Program.LibroMayorALL[i].FormatCode).Replace("\'", " ");
                    lspAcctName = ""; if (ETL.Program.LibroMayorALL[i].AcctName is not null) lspAcctName = Convert.ToString(ETL.Program.LibroMayorALL[i].AcctName).Replace("\'", " ");
                    ls_pPriceBefDi = Convert.ToString(ETL.Program.LibroMayorALL[i].DebitCredit).Replace(',', '.');
                    lspOcrCode1 = ""; if (ETL.Program.LibroMayorALL[i].OcrCode1 is not null) lspOcrCode1 = Convert.ToString(ETL.Program.LibroMayorALL[i].OcrCode1).Replace("\'", " ");
                    lspOcrCode2 = ""; if (ETL.Program.LibroMayorALL[i].OcrCode2 is not null) lspOcrCode2 = Convert.ToString(ETL.Program.LibroMayorALL[i].OcrCode2).Replace("\'", " ");
                    lspOcrCode3 = ""; if (ETL.Program.LibroMayorALL[i].OcrCode3 is not null) lspOcrCode3 = Convert.ToString(ETL.Program.LibroMayorALL[i].OcrCode3).Replace("\'", " ");
                    lspContraAct = ""; if (ETL.Program.LibroMayorALL[i].ContraAct is not null) lspContraAct = Convert.ToString(ETL.Program.LibroMayorALL[i].ContraAct).Replace("\'", " ");
                    lsCountry = ""; if (Convert.ToString(ETL.Program.LibroMayorALL[i].Country) is not null) lspRefDate = Convert.ToString(ETL.Program.LibroMayorALL[i].Country).Replace("\'", " ");

                    lspTransType = ""; if (ETL.Program.LibroMayorALL[i].TransType is not null) lspTransType = Convert.ToString(ETL.Program.LibroMayorALL[i].TransType).Replace("\'", " ");
                    lspContraActName = ""; if (Convert.ToString(ETL.Program.LibroMayorALL[i].ContraActName) is not null) lspContraActName = Convert.ToString(ETL.Program.LibroMayorALL[i].ContraActName).Replace("\'", " ");


                    lspRefDate = ""; 
                    if (Convert.ToString(ETL.Program.LibroMayorALL[i].RefDate) is not null) 
                            lspRefDate = Convert.ToString(ETL.Program.LibroMayorALL[i].RefDate);
                    
                    if (ETL.Program.LibroMayorALL[i].Company == "CLTSTIMPEQ")
                        pEmpresa = "VIELCO";
                    else if (ETL.Program.LibroMayorALL[i].Company == "CLTST_VIELVA")
                        pEmpresa = "VIELVA";
                    else if (ETL.Program.LibroMayorALL[i].Company == "CLTST_VIELCO_PE_2")
                        pEmpresa = "PERU";
                    else if (ETL.Program.LibroMayorALL[i].Company == "CLTST_VICORP")
                        pEmpresa = "VICORP";

                    lsql = "'" + lsCountry  + "'," +
                            "" + ETL.Program.LibroMayorALL[1].TransId  + "," +
                            "'" + lspRefDate + "'," +
                            "'" + lspBaseRef + "'," +
                            "'" + lspFolio + "'," +
                            "'" + lspLineMemo + "'," +
                            "'" + lspFormatCode + "'," +
                            "'" + lspAcctName + "'," +
                            "" + ls_pPriceBefDi + "," +
                            "'" + lspOcrCode1 + "'," +
                            "'" + lspOcrCode2 + "'," +
                            "'" + lspOcrCode3 + "'," +
                            "'" + lspContraAct + "'," +
                            "'" + pEmpresa + "'," +
                            "'" + lspTransType + "'," +
                            "'" + lspContraActName + "'";
                    
                    /*StreamWriter fichero;
                    fichero = File.AppendText("C:\\Users\\carlos.caceres\\OneDrive - Grupo Vielva\\Escritorio\\Nueva carpeta (8)\\data.txt");
                    fichero.WriteLine("select public.fx_carga_libro_mayor (" + lsql + ")");
                    fichero.Close();*/
                    
                    NpgsqlConnection.ClearAllPools();
                    
                    await using (NpgsqlCommand cmd = new NpgsqlCommand("select public.fx_carga_libro_mayor (" + lsql + ")", conn))
                    {
                        await using (NpgsqlDataReader dr = await cmd.ExecuteReaderAsync())
                            while (await dr.ReadAsync())
                            {
                                mje = Convert.ToString(dr[0]);
                                ETL.Program.myContador = ETL.Program.myContador + 1;
                                //dr.Close();
                            }
                    }
                }
                conn.Close();
                NpgsqlConnection.ClearAllPools();
                return mje;
            }
            catch (Exception e)
            {
                err = e.Message;
                NpgsqlConnection.ClearAllPools();
                registrar_error(pEmpresa, 2, "LIBRO MAYOR", e.Message);
                return err;
            }
        }

        public static async Task<string> cargasocios_de_negocios( )
        {
            string err = "";
            string mje = "";
           

            try
            {

                NpgsqlConnection conn = new NpgsqlConnection(ETL.Program._cn);
                conn.Open();

                for (int i = 0; i < ETL.Program.SocNegocioALL.Count; i++)
                {
                    string lspCardName = ""; if (ETL.Program.SocNegocioALL[i].CardName is not null) lspCardName = Convert.ToString(ETL.Program.SocNegocioALL[i].CardName).Replace("\'", " ");
                    string lspAddress = ""; if (ETL.Program.SocNegocioALL[i].Address is not null) lspAddress = Convert.ToString(ETL.Program.SocNegocioALL[i].Address).Replace("\'", " ");
                    string lspCounty = ""; if (ETL.Program.SocNegocioALL[i].County is not null) lspCounty = Convert.ToString(ETL.Program.SocNegocioALL[i].County).Replace("\'", " ");
                    string lspCity = ""; if (ETL.Program.SocNegocioALL[i].City is not null) lspCity = Convert.ToString(ETL.Program.SocNegocioALL[i].City).Replace("\'", " ");
                    string lspCountry = ""; if (ETL.Program.SocNegocioALL[i].Country is not null) lspCountry = Convert.ToString(ETL.Program.SocNegocioALL[i].Country).Replace("\'", " ");
                    string ls_pCreditLine = Convert.ToString(ETL.Program.SocNegocioALL[i].CreditLine).Replace(',', '.');
                    string lsUpdateDate = "";  if (ETL.Program.SocNegocioALL[i].Fecha_Actualizacion is not null) lsUpdateDate = Convert.ToString(ETL.Program.SocNegocioALL[i].Fecha_Actualizacion); 
                    string lsCreateDate = ""; if (ETL.Program.SocNegocioALL[i].Fecha_Creacion is not null) lsCreateDate = Convert.ToString(ETL.Program.SocNegocioALL[i].Fecha_Creacion);
                    string lsql;
                    lsql = "'" + ETL.Program.SocNegocioALL[i].CardCode + "'," +
                            "'" + lspCardName + "'," +
                            "'" + ETL.Program.SocNegocioALL[i].LicTradNum + "'," +
                            "'" + ETL.Program.SocNegocioALL[i].CardType + "'," +
                            "'" + ETL.Program.SocNegocioALL[i].TipoSN + "'," +
                            "'" + ETL.Program.SocNegocioALL[i].Cuenta + "'," +
                            "'" + ETL.Program.SocNegocioALL[i].DesCuenta + "'," +
                            "'" + ETL.Program.SocNegocioALL[i].Currency + "'," +
                            "" + ETL.Program.SocNegocioALL[i].GroupCode + "," +
                            "'" + ETL.Program.SocNegocioALL[i].DesGrupo + "'," +
                            "'" + ETL.Program.SocNegocioALL[i].U_SecFinanciero + "'," +
                            "'" + ETL.Program.SocNegocioALL[i].U_SUBSEGMENTO + "'," +
                            "" + ETL.Program.SocNegocioALL[i].SlpCode + "," +
                            "'" + ETL.Program.SocNegocioALL[i].DesVendedor + "'," +
                            "" + ETL.Program.SocNegocioALL[i].ListNum + "," +
                            "'" + ETL.Program.SocNegocioALL[i].ListaPrecio + "'," +
                            "" + ETL.Program.SocNegocioALL[i].GroupNum + "," +
                            "'" + ETL.Program.SocNegocioALL[i].DesFPago + "'," +
                            "" + ls_pCreditLine + "," +
                            "'" + lspAddress + "'," +
                            "'" + lspCounty + "'," +
                            "'" + lspCity + "'," +
                            "'" + lspCountry + "'," +
                            "'" + ETL.Program.SocNegocioALL[i].Propiedades + "'," +
                            "'" + ETL.Program.SocNegocioALL[i].Empresa + "'," +
                            "'" + lsUpdateDate + "'," +
                            "'" + lsCreateDate + "'";

                    StreamWriter fichero;
                    fichero = File.AppendText("C:\\temp\\data.txt");
                    fichero.WriteLine("select public.fx_carga_socionegocios (" + lsql + ")");
                    fichero.Close();

                    NpgsqlConnection.ClearAllPools();
                    await using (NpgsqlCommand cmd = new NpgsqlCommand("select public.fx_carga_socionegocios (" + lsql + ")", conn))
                    {
                        await using (NpgsqlDataReader dr = await cmd.ExecuteReaderAsync())
                            while (await dr.ReadAsync())
                            {
                                mje = Convert.ToString(dr[0]);
                                ETL.Program.myContador = ETL.Program.myContador + 1;
                                //dr.Close();
                            }
                    }
                    
                }

                conn.Close();
                NpgsqlConnection.ClearAllPools();
                return mje;
            }
            catch (Exception e)
            {
                err = e.Message;
                NpgsqlConnection.ClearAllPools();
                registrar_error("", 2, "cargasocios_de_negocios", e.Message);
                return err;
            }
        }

        public static async Task<string> carga_MAESTRO_BODEGAS()
        {
            string err = "";
            string mje = "";
            try
            {
                NpgsqlConnection conn = new NpgsqlConnection(ETL.Program._cn);
                conn.Open();
                for (int i = 0; i < ETL.Program.MaestoBodegaALL.Count; i++)
                {
                    string lspWhsCode = "";if (ETL.Program.MaestoBodegaALL[i].WhsCode is not null){lspWhsCode = Convert.ToString(ETL.Program.MaestoBodegaALL[i].WhsCode ).Replace("\'", " ");}
                    string lspWhsName = "";if (ETL.Program.MaestoBodegaALL[i].WhsName is not null){lspWhsName = Convert.ToString(ETL.Program.MaestoBodegaALL[i].WhsName).Replace("\'", " ");}
                    string lspCity = "";if (ETL.Program.MaestoBodegaALL[i].City is not null){lspCity = Convert.ToString(ETL.Program.MaestoBodegaALL[i].City).Replace("\'", " ");}
                    string lsql;
                    lsql = "'" + lspWhsCode + "'," +
                            "'" + lspWhsName + "'," +
                            "'" + lspCity + "'," +
                            "'" + ETL.Program.MaestoBodegaALL[i].Empresa  + "'";

                    /*StreamWriter fichero;
                    fichero = File.AppendText("C:\\Users\\carlos.caceres\\OneDrive - Grupo Vielva\\Escritorio\\Nueva carpeta (8)\\data.txt");
                    fichero.WriteLine("select public.carga_MAESTRO_BODEGAS (" + lsql + ")");
                    fichero.Close();*/

                    NpgsqlConnection.ClearAllPools();
                    await using (NpgsqlCommand cmd = new NpgsqlCommand("select public.fx_carga_maesto_bodegas (" + lsql + ")", conn))
                    {
                        await using (NpgsqlDataReader dr = await cmd.ExecuteReaderAsync())
                            while (await dr.ReadAsync())
                            {
                                mje = Convert.ToString(dr[0]);
                                ETL.Program.myContador = ETL.Program.myContador + 1;
                            }
                    }
                }
                conn.Close();
                NpgsqlConnection.ClearAllPools();
                return mje;
            }
            catch (Exception e)
            {
                err = e.Message;
                NpgsqlConnection.ClearAllPools();
                registrar_error("", 2, "carga_MAESTRO_BODEGAS", e.Message);
                return err;
            }
        }

        public static async Task<string> cargaGuias_Facturas()
        {
            string err = "";
            string mje = "";

            try
            {
                string lsql;
                NpgsqlConnection conn = new NpgsqlConnection(ETL.Program._cn);
                conn.Open();

                for (int i = 0; i < ETL.Program.GuiaFacturaALL.Count; i++)
                {
                    /*
                    string ps_DocDate = "1900-01-01";
                    if (ETL.Program.GuiaFacturaALL[i].DocDate != null && ETL.Program.GuiaFacturaALL[i].DocDate != "") ps_DocDate = ETL.Program.GuiaFacturaALL[i].DocDate;

                    string ps_DocDate_OINV = "1900-01-01";
                    if (ETL.Program.GuiaFacturaALL[i].DocDate_OINV != null && ETL.Program.GuiaFacturaALL[i].DocDate_OINV != "") ps_DocDate_OINV = ETL.Program.GuiaFacturaALL[i].DocDate_OINV;
                    */

                    lsql = "'" + ETL.Program.GuiaFacturaALL[i].DocNum + "'," +
                           "'" + ETL.Program.GuiaFacturaALL[i].FolioNum + "'," +
                           "'" + ETL.Program.GuiaFacturaALL[i].U_NXIndTras+ "'," +
                           "'" + ETL.Program.GuiaFacturaALL[i].DocDate + "'," +
                           "'" + ETL.Program.GuiaFacturaALL[i].DocNum_OINV + "'," +
                           "'" + ETL.Program.GuiaFacturaALL[i].FolioNum_OINV + "'," +
                           "'" + ETL.Program.GuiaFacturaALL[i].DocDate_OINV + "'," +
                           "'" + ETL.Program.GuiaFacturaALL[i].Empresa + "'";

                    NpgsqlConnection.ClearAllPools();

                    /*StreamWriter fichero;
                    fichero = File.AppendText("C:\\Users\\carlos.caceres\\OneDrive - Grupo Vielva\\Escritorio\\Nueva carpeta (8)\\data.txt");
                    fichero.WriteLine("select public.fx_carga_guias_facturas (" + lsql + ")");
                    fichero.Close();
                    */

                    await using (NpgsqlCommand cmd = new NpgsqlCommand("select public.fx_carga_guias_facturas (" + lsql + ")", conn))
                    {
                        await using (NpgsqlDataReader dr = await cmd.ExecuteReaderAsync())
                            while (await dr.ReadAsync())
                            {
                                mje = Convert.ToString(dr[0]);
                                ETL.Program.myContador = ETL.Program.myContador + 1;
                                //dr.Close();
                            }
                    }
                }

                conn.Close();
                NpgsqlConnection.ClearAllPools();
                return mje;
            }
            catch (Exception e)
            {
                err = e.Message;
                NpgsqlConnection.ClearAllPools();
                registrar_error("", 2, "cargaPRODUCTOSV200", e.Message);
                return err;
            }

        }

        public static async Task<string> cargaPRODUCTOSV200()
        {
            string err = "";
            string mje = "";

            try
            {
                string lsql;
                NpgsqlConnection conn = new NpgsqlConnection(ETL.Program._cn);
                conn.Open();

                for (int i = 0; i < ETL.Program.ProductosALL.Count; i++)
                {
                    string ps_pCostoArticulo = Convert.ToString(ETL.Program.ProductosALL[i].CostoArticulo).Replace(',', '.');
                    if (ETL.Program.ProductosALL[i].Descripcion == null) ETL.Program.ProductosALL[i].Descripcion = "";

                    string ps_LastPurDat = "1900-01-01";
                    if (ETL.Program.ProductosALL[i].LastPurDat != null && ETL.Program.ProductosALL[i].LastPurDat != "") ps_LastPurDat=ETL.Program.ProductosALL[i].LastPurDat;

                    string ps_UpdateDate = "1900-01-01";
                    if (ETL.Program.ProductosALL[i].UpdateDate != null && ETL.Program.ProductosALL[i].UpdateDate != "") ps_UpdateDate = ETL.Program.ProductosALL[i].UpdateDate;


                    lsql = "'" + ETL.Program.ProductosALL[i].Empresa + "'," +
                           "'" + ETL.Program.ProductosALL[i].Id_Articulo + "'," +
                           "'" + ETL.Program.ProductosALL[i].Descripcion.Replace("'", "") + "'," +
                           "'" + ETL.Program.ProductosALL[i].ArtInventario + "'," +
                           "'" + ETL.Program.ProductosALL[i].ArtVenta + "'," +
                           "'" + ETL.Program.ProductosALL[i].ArtCompra + "'," +
                           "'" + ETL.Program.ProductosALL[i].GrupoArticulo + "'," +
                           "'" + ETL.Program.ProductosALL[i].Activo + "'," +
                           "'" + ETL.Program.ProductosALL[i].Inactivo + "'," +
                           "'" + ETL.Program.ProductosALL[i].TipListaMaterial + "'," +
                           "'" + ETL.Program.ProductosALL[i].Propiedades + "'," +
                           "'" + ETL.Program.ProductosALL[i].U_Familia + "'," +
                           "'" + ETL.Program.ProductosALL[i].U_Subfamilia + "'," +
                           "'" + ETL.Program.ProductosALL[i].U_SubSubfamilia + "'," +
                           "'" + ETL.Program.ProductosALL[i].Clasif_Comercial + "'," +
                           "" + Convert.ToInt32(ETL.Program.ProductosALL[i].IdGrupoArticulo) + "," +
                           "" + ps_pCostoArticulo + "," +
                           "'" + ETL.Program.ProductosALL[i].CardCode + "',"+
                           
                           "'" + ps_LastPurDat + "'," +
                            "'" + ps_UpdateDate  + "'";

                    NpgsqlConnection.ClearAllPools();

                    /*
                  StreamWriter fichero;
                  fichero = File.AppendText("C:\\Users\\carlos.caceres\\OneDrive - Grupo Vielva\\Escritorio\\Nueva carpeta (8)\\data.txt");
                  fichero.WriteLine("select public.fx_carga_productos (" + lsql + ")");
                  fichero.Close();
                    */
                    await using (NpgsqlCommand cmd = new NpgsqlCommand("select public.fx_carga_productos (" + lsql + ")", conn))
                    {
                        await using (NpgsqlDataReader dr = await cmd.ExecuteReaderAsync())
                            while (await dr.ReadAsync())
                            {
                                mje = Convert.ToString(dr[0]);
                                ETL.Program.myContador = ETL.Program.myContador + 1;
                                //dr.Close();
                            }
                    }
                }

                conn.Close();
                NpgsqlConnection.ClearAllPools();
                return mje;
            }
            catch (Exception e)
            {
                err = e.Message;
                NpgsqlConnection.ClearAllPools();
                registrar_error("", 2, "cargaPRODUCTOSV200", e.Message);
                return err;
            }

        }

        public static async Task<string> carga_lista_de_precios()
        {
            string err = "";
            string mje = "";
            try
            {
                NpgsqlConnection conn = new NpgsqlConnection(ETL.Program._cn);
                conn.Open();

                for (int i = 0; i < ETL.Program.ListaPrecioALL.Count; i++)
                {
                    string ls_pPrecio = Convert.ToString(ETL.Program.ListaPrecioALL[i].Precio).Replace(',', '.');
                    string lspDescripcion = "";
                    if (ETL.Program.ListaPrecioALL[i].Descripcion is not null)
                        lspDescripcion = Convert.ToString(ETL.Program.ListaPrecioALL[i].Descripcion).Replace("\'", " ");

                    string lsql;
                    lsql = "'" + ETL.Program.ListaPrecioALL[i].Numero_Articulo  + "'," +
                            "'" + lspDescripcion + "'," +
                            "" + ls_pPrecio + "," +
                            "'" + ETL.Program.ListaPrecioALL[i].Empresa  + "'";

                    /*StreamWriter fichero;
                    fichero = File.AppendText("C:\\Users\\carlos.caceres\\OneDrive - Grupo Vielva\\Escritorio\\Nueva carpeta (8)\\data.txt");
                    fichero.WriteLine("select public.fx_carga_lista_precios (" + lsql + ")");
                    fichero.Close();*/

                    NpgsqlConnection.ClearAllPools();


                    await using (NpgsqlCommand cmd = new NpgsqlCommand("select public.fx_carga_lista_precios (" + lsql + ")", conn))
                    {
                        await using (NpgsqlDataReader dr = await cmd.ExecuteReaderAsync())
                            while (await dr.ReadAsync())
                            {
                                mje = Convert.ToString(dr[0]);
                                ETL.Program.myContador = ETL.Program.myContador + 1;
                                //dr.Close();
                            }
                    }

                }
                conn.Close();
                NpgsqlConnection.ClearAllPools();
                return mje;
            }
            catch (Exception e)
            {
                err = e.Message;
                NpgsqlConnection.ClearAllPools();
                registrar_error("", 2, "carga_lista_de_precios", e.Message);
                return err;
            }
        }

        public static async Task<string> carga_stock_productos()
        {
            string err = "";
            string mje = "";

            try
            {

                NpgsqlConnection conn = new NpgsqlConnection(ETL.Program._cn);
                string lsql;
                conn.Open();
                
                for (int i = 0; i < ETL.Program.StockProductoALL.Count; i++)
                {
                    string ls_pOnHand = Convert.ToString(ETL.Program.StockProductoALL[i].OnHand).Replace(',', '.');
                    string ls_pIsCommited = Convert.ToString(ETL.Program.StockProductoALL[i].IsCommited).Replace(',', '.');
                    string ls_pOnOrder = Convert.ToString(ETL.Program.StockProductoALL[i].OnOrder).Replace(',', '.');

                    string lspItemName = "";
                    if (ETL.Program.StockProductoALL[i].ItemName is not null)
                        lspItemName = Convert.ToString(ETL.Program.StockProductoALL[i].ItemName).Replace("\'", " ");

                    lsql = "'" + ETL.Program.StockProductoALL[i].ItemCode  + "'," +
                            "'" + lspItemName + "'," +
                            "'" + ETL.Program.StockProductoALL[i].WhsCode + "'," +
                            "'" + ETL.Program.StockProductoALL[i].WhsName + "'," +
                            "" + ls_pOnHand + "," +
                            "" + ls_pIsCommited + "," +
                            "" + ls_pOnOrder + "," +
                            "'" + ETL.Program.StockProductoALL[i].Empresa + "'";


                    /*StreamWriter fichero;
                    fichero = File.AppendText("C:\\Users\\carlos.caceres\\OneDrive - Grupo Vielva\\Escritorio\\Nueva carpeta (8)\\data.txt");
                    fichero.WriteLine("select public.fx_carga_stock_productos (" + lsql + ")");
                    fichero.Close();*/

                    NpgsqlConnection.ClearAllPools();


                    await using (NpgsqlCommand cmd = new NpgsqlCommand("select public.fx_carga_stock_productos (" + lsql + ")", conn))
                    {
                        await using (NpgsqlDataReader dr = await cmd.ExecuteReaderAsync())
                            while (await dr.ReadAsync())
                            {
                                mje = Convert.ToString(dr[0]);
                                ETL.Program.myContador = ETL.Program.myContador + 1;
                                //dr.Close();
                            }
                    }

                }
                conn.Close();
                NpgsqlConnection.ClearAllPools();
                return mje;
            }
            catch (Exception e)
            {
                err = e.Message;
                NpgsqlConnection.ClearAllPools();
                registrar_error("", 2, "carga_stock_productos", e.Message);
                return err;
            }
        }

        public static async Task<string> carga_nventas_digitadas()
        {
            string err = "";
            string mje = "";

            try
            {
                string lsql;
                NpgsqlConnection conn = new NpgsqlConnection(ETL.Program._cn);
                conn.Open();
                for (int i = 0; i < ETL.Program.NVtaDigALL.Count; i++)
                {
                    //my_retorno = await PBI.clsMaestroProductos.carga_nventas_digitadas(ETL.Program.NVtaDigALL[i].DocNum
                    string ls_pPriceBefDi = Convert.ToString(ETL.Program.NVtaDigALL[i].PriceBefDi).Replace(',', '.');
                    string ls_pPrice = Convert.ToString(ETL.Program.NVtaDigALL[i].Price).Replace(',', '.');
                    string ls_pQuantity = Convert.ToString(ETL.Program.NVtaDigALL[i].Quantity).Replace(',', '.');
                    string ls_pDelivrdQty = Convert.ToString(ETL.Program.NVtaDigALL[i].DelivrdQty).Replace(',', '.');
                    string ls_pLineTotal = Convert.ToString(ETL.Program.NVtaDigALL[i].LineTotal).Replace(',', '.');
                    string ls_pPorcCumplimiento = Convert.ToString(ETL.Program.NVtaDigALL[i].PorcCumplimiento).Replace(',', '.');

                    string lsCardName = "";if (ETL.Program.NVtaDigALL[i].CardName is not null)lsCardName = Convert.ToString(ETL.Program.NVtaDigALL[i].CardName).Replace("\'", " ");
                    string lsSituacion = "";if (ETL.Program.NVtaDigALL[i].Situacion is not null)lsSituacion = Convert.ToString(ETL.Program.NVtaDigALL[i].Situacion).Replace("\'", " ");
                    string lsDscription = "";if (ETL.Program.NVtaDigALL[i].Dscription is not null)lsDscription = Convert.ToString(ETL.Program.NVtaDigALL[i].Dscription).Replace("\'", " ");
                    string lsSlpName = "";if (ETL.Program.NVtaDigALL[i].SlpName is not null)lsSlpName = Convert.ToString(ETL.Program.NVtaDigALL[i].SlpName).Replace("\'", " ");
                    string lsU_TipoOV = ""; if (ETL.Program.NVtaDigALL[i].U_TipoOV is not null) lsU_TipoOV = Convert.ToString(ETL.Program.NVtaDigALL[i].U_TipoOV).Replace("\'", " ");

                    lsql = "'" + ETL.Program.NVtaDigALL[i].DocNum + "'," +
                            "'" + ETL.Program.NVtaDigALL[i].CardCode + "'," +
                            "'" + lsCardName + "'," +
                            "'" + ETL.Program.NVtaDigALL[i].DocDate + "'," +
                            "'" + ETL.Program.NVtaDigALL[i].FechaEntrega + "'," +
                            "'" + ETL.Program.NVtaDigALL[i].ClaseExpedicion + "'," +
                            "'" + lsSituacion + "'," +
                            "'" + ETL.Program.NVtaDigALL[i].OC + "'," +
                            "'" + ETL.Program.NVtaDigALL[i].ItemCode + "'," +
                            "'" + ETL.Program.NVtaDigALL[i].Dscription + "'," +
                            "'" + ETL.Program.NVtaDigALL[i].WhsCode + "'," +
                            "" + ls_pPriceBefDi + "," +
                            "" + ls_pPrice + "," +
                            "" + ls_pQuantity + "," +
                            "" + ls_pDelivrdQty + "," +
                            "" + ls_pLineTotal + "," +
                            "" + ls_pPorcCumplimiento + "," +
                            "'" + ETL.Program.NVtaDigALL[i].SlpCode + "'," +
                            "'" + lsSlpName + "'," +
                            "'" + ETL.Program.NVtaDigALL[i].OcrCode + "'," +
                            "'" + ETL.Program.NVtaDigALL[i].OcrCode2 + "'," +
                            "'" + ETL.Program.NVtaDigALL[i].OcrCode3 + "'," +
                            
                            "'" + lsU_TipoOV + "'," +
                            "'" + ETL.Program.NVtaDigALL[i].Empresa + "'";

                    NpgsqlConnection.ClearAllPools();
                    await using (NpgsqlCommand cmd = new NpgsqlCommand("select public.fx_carga_nota_vtas_digitadas (" + lsql + ")", conn))
                    {
                        await using (NpgsqlDataReader dr = await cmd.ExecuteReaderAsync())
                            while (await dr.ReadAsync())
                            {
                                mje = Convert.ToString(dr[0]);
                                ETL.Program.myContador = ETL.Program.myContador + 1;
                                //dr.Close();
                            }
                    }

                }


                conn.Close();
                NpgsqlConnection.ClearAllPools();
                return mje;
            }
            catch (Exception e)
            {
                err = e.Message;
                NpgsqlConnection.ClearAllPools();
                registrar_error("", 2, "carga_nventas_digitadas", e.Message);
                return err;
            }
        }

        public static async Task<string> carga_oferta_ventas()
        {
            string err = "";
            string mje = "";

            try
            {
                string lsql;
                NpgsqlConnection conn = new NpgsqlConnection(ETL.Program._cn);
                conn.Open();
                for (int i = 0; i < ETL.Program.OfertaVtaALL.Count; i++)
                {
                    string ls_pDocRate = Convert.ToString(ETL.Program.OfertaVtaALL[i].DocRate).Replace(',', '.');
                    string ls_pQuantity = Convert.ToString(ETL.Program.OfertaVtaALL[i].Quantity).Replace(',', '.');
                    string ls_pOpenQty = Convert.ToString(ETL.Program.OfertaVtaALL[i].OpenQty).Replace(',', '.');
                    string ls_pPrice = Convert.ToString(ETL.Program.OfertaVtaALL[i].Price).Replace(',', '.');
                    string ls_pRate = Convert.ToString(ETL.Program.OfertaVtaALL[i].Rate).Replace(',', '.');
                    string ls_pLineTotal = Convert.ToString(ETL.Program.OfertaVtaALL[i].LineTotal).Replace(',', '.');
                    string ls_pOpenSum = Convert.ToString(ETL.Program.OfertaVtaALL[i].OpenSum).Replace(',', '.');

                    
                    lsql = "" + ETL.Program.OfertaVtaALL[i].DocNum + "," +
                            "'" + ETL.Program.OfertaVtaALL[i].DocType + "'," +
                            "'" + ETL.Program.OfertaVtaALL[i].CANCELED + "'," +
                            "'" + ETL.Program.OfertaVtaALL[i].DocStatus + "'," +
                            "'" + ETL.Program.OfertaVtaALL[i].ObjType + "'," +
                            "'" + ETL.Program.OfertaVtaALL[i].DocDate + "'," +
                            "'" + ETL.Program.OfertaVtaALL[i].CardCode + "'," +
                            "'" + ETL.Program.OfertaVtaALL[i].CardName + "'," +
                            "'" + ETL.Program.OfertaVtaALL[i].DocCur + "'," +
                            "" + ls_pDocRate + "," +
                            "" + ETL.Program.OfertaVtaALL[i].DocEntry + "," +
                            "" + ETL.Program.OfertaVtaALL[i].LineNum + "," +
                            "" + ETL.Program.OfertaVtaALL[i].TargetType + "," +
                            "'" + ETL.Program.OfertaVtaALL[i].LineStatus + "'," +
                            "'" + ETL.Program.OfertaVtaALL[i].ItemCode + "'," +
                            "'" + ETL.Program.OfertaVtaALL[i].Dscription + "'," +
                            "" + ls_pQuantity + "," +
                            "" + ls_pOpenQty + "," +
                            "" + ls_pPrice + "," +
                            "'" + ETL.Program.OfertaVtaALL[i].Currency + "'," +
                            "" + ls_pRate + "," +
                            "" + ls_pLineTotal + "," +
                            "" + ls_pOpenSum + "," +
                            "'" + ETL.Program.OfertaVtaALL[i].Fecha_ETA + "'," +
                            "'" + ETL.Program.OfertaVtaALL[i].Fecha_ETD + "'," +
                            "'" + ETL.Program.OfertaVtaALL[i].Ref_Acreedor + "'," +
                            "'" + ETL.Program.OfertaVtaALL[i].Empresa + "'";

                    NpgsqlConnection.ClearAllPools();

                    await using (NpgsqlCommand cmd = new NpgsqlCommand("select public.fx_carga_ofertaventas (" + lsql + ")", conn))
                    {
                        await using (NpgsqlDataReader dr = await cmd.ExecuteReaderAsync())
                            while (await dr.ReadAsync())
                            {
                                mje = Convert.ToString(dr[0]);
                                ETL.Program.myContador = ETL.Program.myContador + 1;
                                //dr.Close();
                            }
                    }
                }


                conn.Close();
                NpgsqlConnection.ClearAllPools();
                return mje;
            }
            catch (Exception e)
            {
                err = e.Message;
                NpgsqlConnection.ClearAllPools();
                registrar_error("", 2, "carga_oferta_ventas", e.Message);
                return err;
            }
        }
        public static async Task<string> carga_factura_reserva()
        {
            string err = "";
            string mje = "";

            try
            {
                string lsql;
                NpgsqlConnection conn = new NpgsqlConnection(ETL.Program._cn);
                conn.Open();
                for (int i = 0; i < ETL.Program.FacturaReservALL.Count; i++)
                {
                    string ls_pCant = Convert.ToString(ETL.Program.FacturaReservALL[i].Cant).Replace(',', '.');
                    string ls_pPrecio = Convert.ToString(ETL.Program.FacturaReservALL[i].Precio).Replace(',', '.');
                    string ls_pTotal_LInea = Convert.ToString(ETL.Program.FacturaReservALL[i].Total_LInea).Replace(',', '.');
                    string ls_pCant_Pendiente = Convert.ToString(ETL.Program.FacturaReservALL[i].Cant_Pendiente).Replace(',', '.');

                    lsql = "" + ETL.Program.FacturaReservALL[i].DocEntry + "," +
                            "" + ETL.Program.FacturaReservALL[i].DocNum + "," +
                            "'" + ETL.Program.FacturaReservALL[i].DocType + "'," +
                            "'" + ETL.Program.FacturaReservALL[i].CardCode + "'," +
                            "'" + ETL.Program.FacturaReservALL[i].CardName + "'," +
                            "'" + ETL.Program.FacturaReservALL[i].Estado + "'," +
                            "'" + ETL.Program.FacturaReservALL[i].Id_Producto + "'," +
                            "'" + ETL.Program.FacturaReservALL[i].Producto + "'," +
                            "'" + ETL.Program.FacturaReservALL[i].Dscription + "'," +
                            "" + ls_pCant + "," +
                            "" + ls_pPrecio + "," +
                            "" + ls_pTotal_LInea + "," +
                            "'" + ETL.Program.FacturaReservALL[i].Moneda + "'," +
                            "'" + ETL.Program.FacturaReservALL[i].BaseRef + "'," +
                            "'" + ETL.Program.FacturaReservALL[i].U_Fecha_ETA + "'," +
                            "'" + ETL.Program.FacturaReservALL[i].NumAtCard + "'," +
                            "" + ls_pCant_Pendiente + "," +
                            "'" + ETL.Program.FacturaReservALL[i].isIns + "'," +

                            "'" + ETL.Program.FacturaReservALL[i].Fecha + "'," +
                            "'" + ETL.Program.FacturaReservALL[i].ETD + "'," +

                            "'" + ETL.Program.FacturaReservALL[i].Empresa + "'";


                    NpgsqlConnection.ClearAllPools();


                    await using (NpgsqlCommand cmd = new NpgsqlCommand("select public.fx_carga_facturas_reservas (" + lsql + ")", conn))
                    {
                        await using (NpgsqlDataReader dr = await cmd.ExecuteReaderAsync())
                            while (await dr.ReadAsync())
                            {
                                mje = Convert.ToString(dr[0]);
                                ETL.Program.myContador = ETL.Program.myContador + 1;
                                //dr.Close();
                            }

                    }

                }


                conn.Close();
                NpgsqlConnection.ClearAllPools();
                return mje;
            }
            catch (Exception e)
            {
                err = e.Message;
                NpgsqlConnection.ClearAllPools();
                registrar_error("", 2, "carga_factura_reserva", e.Message);
                return err;
            }
        }
        public static async Task<string> cargaPartidasAbiertas()
        {
            string err = "";
            string mje = "";

            try
            {
                string lsql;
                NpgsqlConnection conn = new NpgsqlConnection(ETL.Program._cn);
                conn.Open();

                for (int i = 0; i < ETL.Program.PartidasAbiertasALL.Count; i++)
                {
                    string ls_pCantidad = Convert.ToString(ETL.Program.PartidasAbiertasALL[i].Cantidad).Replace(',', '.');
                    string ls_pPrecio_por_unidad = Convert.ToString(ETL.Program.PartidasAbiertasALL[i].Precio_por_unidad).Replace(',', '.');
                    string ls_pPorc_descuento = Convert.ToString(ETL.Program.PartidasAbiertasALL[i].Porc_descuento).Replace(',', '.');
                    string ls_pTotal_ML = Convert.ToString(ETL.Program.PartidasAbiertasALL[i].Total_ML).Replace(',', '.');
                    string ls_pEn_Stock = Convert.ToString(ETL.Program.PartidasAbiertasALL[i].En_Stock).Replace(',', '.');
                    string ls_pComprometido = Convert.ToString(ETL.Program.PartidasAbiertasALL[i].Comprometido).Replace(',', '.');
                    string lspDestino = "";if (ETL.Program.PartidasAbiertasALL[i].Destino is not null) lspDestino = Convert.ToString(ETL.Program.PartidasAbiertasALL[i].Destino).Replace("\'", " ");
                    string lspDescripcion_destinatario = "";if (ETL.Program.PartidasAbiertasALL[i].Descripcion_destinatario is not null)lspDescripcion_destinatario = Convert.ToString(ETL.Program.PartidasAbiertasALL[i].Descripcion_destinatario).Replace("\'", " ");
                    string lsU_NXIndTras = ""; if (ETL.Program.PartidasAbiertasALL[i].U_NXIndTras is not null) lsU_NXIndTras = Convert.ToString(ETL.Program.PartidasAbiertasALL[i].U_NXIndTras).Replace("\'", " ");
                    
                    lsql = "" + ETL.Program.PartidasAbiertasALL[i].N_Documento + "," +
                            "'" + ETL.Program.PartidasAbiertasALL[i].N_Folio + "'," +
                            "'" + ETL.Program.PartidasAbiertasALL[i].Rut_Cliente + "'," +
                            "'" + ETL.Program.PartidasAbiertasALL[i].Nombre_Cliente + "'," +
                            "" + ETL.Program.PartidasAbiertasALL[i].Clase_Doc_Destino + "," +
                            "" + ETL.Program.PartidasAbiertasALL[i].Clase_Doc_Base + "," +
                            "'" + ETL.Program.PartidasAbiertasALL[i].Moneda + "'," +
                            "'" + ETL.Program.PartidasAbiertasALL[i].Fecha_Vencimiento + "'," +
                            "'" + ETL.Program.PartidasAbiertasALL[i].Fecha_Contabilizacion + "'," +
                            "'" + ETL.Program.PartidasAbiertasALL[i].Fecha_Documento + "'," +
                            "'" + lspDestino + "'," +
                            "'" + ETL.Program.PartidasAbiertasALL[i].N_articulo + "'," +
                            "'" + ETL.Program.PartidasAbiertasALL[i].Descripcion_articulo + "'," +
                            "" + ls_pCantidad + "," +
                            "" + ls_pPrecio_por_unidad + "," +
                            "" + ls_pPorc_descuento + "," +
                            "'" + ETL.Program.PartidasAbiertasALL[i].Indicador_impuestos + "'," +
                            "" + ls_pTotal_ML + "," +
                            "'" + ETL.Program.PartidasAbiertasALL[i].Almacen + "'," +
                            "'" + ETL.Program.PartidasAbiertasALL[i].Empleado_ventas + "'," +
                            "" + ls_pEn_Stock + "," +
                            "" + ls_pComprometido + "," +
                            "'" + ETL.Program.PartidasAbiertasALL[i].Direccion + "'," +
                            "'" + ETL.Program.PartidasAbiertasALL[i].Detalles_de_articulo + "'," +
                            "'" + ETL.Program.PartidasAbiertasALL[i].Propietario + "'," +
                            "'" + ETL.Program.PartidasAbiertasALL[i].Nombre_destinatario + "'," +
                            "'" + lspDescripcion_destinatario + "'," +
                            "'" + ETL.Program.PartidasAbiertasALL[i].Norma_de_reparto + "'," +
                            "'" + ETL.Program.PartidasAbiertasALL[i].Norma_reparto_precios_coste + "'," +
                            "'" + ETL.Program.PartidasAbiertasALL[i].Codigo_u_medida + "'," +

                            "'" + lsU_NXIndTras + "'," +
                            "'" + ETL.Program.PartidasAbiertasALL[i].Empresa + "'";

                    NpgsqlConnection.ClearAllPools();


                    await using (NpgsqlCommand cmd = new NpgsqlCommand("select public.fx_carga_partidasabiertas (" + lsql + ")", conn))
                    {
                        await using (NpgsqlDataReader dr = await cmd.ExecuteReaderAsync())
                            while (await dr.ReadAsync())
                            {
                                mje = Convert.ToString(dr[0]);
                                ETL.Program.myContador = ETL.Program.myContador + 1;
                                //dr.Close();
                            }
                    }
                }

                conn.Close();
                NpgsqlConnection.ClearAllPools();
                return mje;
            }
            catch (Exception e)
            {
                err = e.Message;
                NpgsqlConnection.ClearAllPools();
                registrar_error("", 2, "cargaPartidasAbiertas", e.Message);
                return err;
            }
        }

        public static async Task<string> carga_pedidos_bloqueados()
        {
            string err = "";
            string mje = "";

            try
            {
                string lsql;
                NpgsqlConnection conn = new NpgsqlConnection(ETL.Program._cn);
                conn.Open();

                for (int i = 0; i < ETL.Program.PedidoBloqALL.Count; i++)
                {
                    string ls_pDiscPrcnt = Convert.ToString(ETL.Program.PedidoBloqALL[i].DiscPrcnt).Replace(',', '.');
                    string ls_pDocTotal = Convert.ToString(ETL.Program.PedidoBloqALL[i].DocTotal).Replace(',', '.');
                    string ls_pDocRate = Convert.ToString(ETL.Program.PedidoBloqALL[i].DocRate).Replace(',', '.');
                    string ls_pQuantity = Convert.ToString(ETL.Program.PedidoBloqALL[i].Quantity).Replace(',', '.');
                    string ls_pPrecio_Unidad = Convert.ToString(ETL.Program.PedidoBloqALL[i].Precio_Unidad).Replace(',', '.');
                    string ls_pDescuento = Convert.ToString(ETL.Program.PedidoBloqALL[i].Descuento).Replace(',', '.');
                    string ls_pPrecioTrasDesco = Convert.ToString(ETL.Program.PedidoBloqALL[i].PrecioTrasDesco).Replace(',', '.');
                    string ls_pLineTotal = Convert.ToString(ETL.Program.PedidoBloqALL[i].LineTotal).Replace(',', '.');

                    lsql = "'" + ETL.Program.PedidoBloqALL[i].ObjType + "'," +
                            "" + ETL.Program.PedidoBloqALL[i].DoctoPreliminar + "," +
                            "" + ETL.Program.PedidoBloqALL[i].NumeroDocumento + "," +
                            "'" + ETL.Program.PedidoBloqALL[i].Estado + "'," +
                            "'" + ETL.Program.PedidoBloqALL[i].WddStatus + "'," +
                            "'" + ETL.Program.PedidoBloqALL[i].ClaseDocumento + "'," +
                            "'" + ETL.Program.PedidoBloqALL[i].Cancelada + "'," +
                            "'" + ETL.Program.PedidoBloqALL[i].Estados + "'," +
                            "'" + ETL.Program.PedidoBloqALL[i].Documento + "'," +
                            "'" + ETL.Program.PedidoBloqALL[i].DocDate + "'," +
                            "'" + ETL.Program.PedidoBloqALL[i].DocDueDate + "'," +
                            "'" + ETL.Program.PedidoBloqALL[i].CardCode + "'," +
                            "'" + ETL.Program.PedidoBloqALL[i].CardName + "'," +
                            "" + ls_pDiscPrcnt + "," +
                            "" + ls_pDocTotal + "," +
                            "'" + ETL.Program.PedidoBloqALL[i].DocCur + "'," +
                            "'" + ETL.Program.PedidoBloqALL[i].Comments + "'," +
                            "" + ls_pDocRate + "," +
                            "'" + ETL.Program.PedidoBloqALL[i].ItemCode + "'," +
                            "'" + ETL.Program.PedidoBloqALL[i].Dscription + "'," +
                            "" + ls_pQuantity + "," +
                            "'" + ETL.Program.PedidoBloqALL[i].ShipDate + "'," +
                            "'" + ETL.Program.PedidoBloqALL[i].Currency + "'," +
                            "" + ls_pPrecio_Unidad + "," +
                            "" + ls_pDescuento + "," +
                            "" + ls_pPrecioTrasDesco + "," +
                            "" + ls_pLineTotal + "," +
                            "'" + ETL.Program.PedidoBloqALL[i].e_aPROB + "'," +
                            "'" + ETL.Program.PedidoBloqALL[i].Status + "'," +
                            "" + ETL.Program.PedidoBloqALL[i].SlpCode + "," +
                            "" + ETL.Program.PedidoBloqALL[i].LineNum + "," +
                            "'" + ETL.Program.PedidoBloqALL[i].Empresa + "'";

                    NpgsqlConnection.ClearAllPools();
                    
                    await using (NpgsqlCommand cmd = new NpgsqlCommand("select public.fx_carga_pedidod_bloqueados (" + lsql + ")", conn))
                    {
                        await using (NpgsqlDataReader dr = await cmd.ExecuteReaderAsync())
                            while (await dr.ReadAsync())
                            {
                                mje = Convert.ToString(dr[0]);
                                ETL.Program.myContador = ETL.Program.myContador + 1;
                                //dr.Close();
                            }
                    }
                }


                conn.Close();
                NpgsqlConnection.ClearAllPools();
                return mje;
            }
            catch (Exception e)
            {
                err = e.Message;
                NpgsqlConnection.ClearAllPools();
                registrar_error("", 2, "carga_pedidos_bloqueados", e.Message);
                return err;
            }
        }

        public static async Task<string> carga_sstt_reportes()
        {
            string err = "";
            string mje = "";

            try
            {
                string lsql;
                NpgsqlConnection conn = new NpgsqlConnection(ETL.Program._cn);
                conn.Open();

                for (int i = 0; i < ETL.Program.SSTTrepALL.Count; i++)
                {
                    string lspcomentario = "";if (ETL.Program.SSTTrepALL[i].Comentario is not null) lspcomentario = Convert.ToString(ETL.Program.SSTTrepALL[i].Comentario).Replace("\'", " ");
                    string lspResolucion = "";if (ETL.Program.SSTTrepALL[i].Resolucion is not null)lspResolucion = Convert.ToString(ETL.Program.SSTTrepALL[i].Resolucion).Replace("\'", " ");
                    string lspDocIngreso = ""; if (ETL.Program.SSTTrepALL[i].DocIngreso is not null) lspDocIngreso = Convert.ToString(ETL.Program.SSTTrepALL[i].DocIngreso).Replace("\'", " ");
                    string lspsubject = ""; if (ETL.Program.SSTTrepALL[i].subject is not null) lspsubject = Convert.ToString(ETL.Program.SSTTrepALL[i].subject).Replace("\'", " ");

                    lspDocIngreso = Convert.ToString(lspDocIngreso).Replace(",", " ");
                    lspsubject = Convert.ToString(lspsubject).Replace(",", " ");
                    lspResolucion = Convert.ToString(lspResolucion).Replace(",", " ");
                    lspcomentario = Convert.ToString(lspcomentario).Replace(",", " ");
                    string lspUPMResp = ""; if (ETL.Program.SSTTrepALL[i].U_PM_Resp is not null) lspUPMResp = Convert.ToString(ETL.Program.SSTTrepALL[i].U_PM_Resp);
                    string lspUFechagGtiProv = ""; if (ETL.Program.SSTTrepALL[i].U_Fecha_g_Gti_Prov is not null) lspUFechagGtiProv = Convert.ToString(ETL.Program.SSTTrepALL[i].U_Fecha_g_Gti_Prov);
                    string lspUReclaFabr = ""; if (ETL.Program.SSTTrepALL[i].subject is not null) lspUReclaFabr = Convert.ToString(ETL.Program.SSTTrepALL[i].U_Recla_Fabr);
                    string lspFeFabresGtia = ""; if (ETL.Program.SSTTrepALL[i].subject is not null) lspFeFabresGtia = Convert.ToString(ETL.Program.SSTTrepALL[i].U_Fe_Fab_res_Gtia);
                    string lspFRecpRepGtia = ""; if (ETL.Program.SSTTrepALL[i].subject is not null) lspFRecpRepGtia = Convert.ToString(ETL.Program.SSTTrepALL[i].U_F_Recp_Rep_Gtia);
                    string lspNombreTecnico = ""; if (ETL.Program.SSTTrepALL[i].subject is not null) lspNombreTecnico = Convert.ToString(ETL.Program.SSTTrepALL[i].NombreTecnico);
                    string lspApellidoTecnico = ""; if (ETL.Program.SSTTrepALL[i].ApellidoTecnico is not null) lspApellidoTecnico = Convert.ToString(ETL.Program.SSTTrepALL[i].ApellidoTecnico);



                    lsql = "" + ETL.Program.SSTTrepALL[i].OrdenTrabajo + "," +
                            "'" + ETL.Program.SSTTrepALL[i].Estado + "'," +
                            "'" + ETL.Program.SSTTrepALL[i].Prioridad + "'," +
                            "'" + ETL.Program.SSTTrepALL[i].Cliente + "'," +
                            "'" + ETL.Program.SSTTrepALL[i].NombreCliente + "'," +
                            "'" + ETL.Program.SSTTrepALL[i].SKU + "'," +
                            "'" + ETL.Program.SSTTrepALL[i].NombreSKU + "'," +
                            "'" + ETL.Program.SSTTrepALL[i].NumeroSerie + "'," +
                            "'" + ETL.Program.SSTTrepALL[i].NumeroSerieFabricante + "'," +
                            "'" + ETL.Program.SSTTrepALL[i].cntrctDate + "'," +
                            "'" + ETL.Program.SSTTrepALL[i].Origen + "'," +
                            "'" + ETL.Program.SSTTrepALL[i].TipoProblema + "'," +
                            "'" + ETL.Program.SSTTrepALL[i].TipoOT + "'," +
                            //"'" + ETL.Program.SSTTrepALL[i].DocIngreso + "'," +
                            "'" + lspDocIngreso + "'," +
                            "'" + lspcomentario + "'," +
                            "'" + lspResolucion + "'," +
                            "'" + ETL.Program.SSTTrepALL[i].createDate + "'," +
                            "'" + ETL.Program.SSTTrepALL[i].closeDate + "'," +
                            "" + ETL.Program.SSTTrepALL[i].contctCode + "," +
                            //"'" + ETL.Program.SSTTrepALL[i].subject + "'," +
                            "'" + lspsubject + "'," +
                            "'" + lspNombreTecnico + "'," +
                            "'" + lspApellidoTecnico + "'," +
                            "" + ETL.Program.SSTTrepALL[i].SubTipoProblema + "," +
                            "" + ETL.Program.SSTTrepALL[i].DocNum + "," +
                            "'" + ETL.Program.SSTTrepALL[i].Sucursal  + "'," +
                            "'" + ETL.Program.SSTTrepALL[i].U_Horometro + "'," +
                            "'" + ETL.Program.SSTTrepALL[i].U_CodigoCliente + "'," +
                            "'" + ETL.Program.SSTTrepALL[i].U_FechaHorometro + "'," +
                            "'" + ETL.Program.SSTTrepALL[i].U_Emergencias + "'," +
                            "'" + ETL.Program.SSTTrepALL[i].U_Ubicacion_Equipo + "'," +
                            "'" + ETL.Program.SSTTrepALL[i].U_Estado_Equipo+ "'," +
                            "'" + ETL.Program.SSTTrepALL[i].U_Recl_Fabr + "'," +
                            "'" + ETL.Program.SSTTrepALL[i].U_Fabr_Res_Gar + "'," +
                            "'" + ETL.Program.SSTTrepALL[i].U_Gestion_Gtia_prov + "'," +
                            "'" + ETL.Program.SSTTrepALL[i].updateDate + "'," +
                            "'" + lspUPMResp + "'," +
                            "'" + lspUFechagGtiProv + "'," +
                            "'" + lspUReclaFabr + "'," +
                            "'" + lspFeFabresGtia + "'," +
                            "'" + lspFRecpRepGtia + "'," +
                            "'" + ETL.Program.SSTTrepALL[i].Empresa + "'";



                    StreamWriter fichero;
                    fichero = File.AppendText("C:\\ETL_FUSION\\LOG\\data.txt");
                    fichero.WriteLine("select public.fx_carga_sstt_reporteot (" + lsql + ")");
                    fichero.Close();
                    

                    NpgsqlConnection.ClearAllPools();
                    await using (NpgsqlCommand cmd = new NpgsqlCommand("select public.fx_carga_sstt_reporteot (" + lsql + ")", conn))
                    {
                        await using (NpgsqlDataReader dr = await cmd.ExecuteReaderAsync())
                            while (await dr.ReadAsync())
                            {
                                mje = Convert.ToString(dr[0]);
                                ETL.Program.myContador = ETL.Program.myContador + 1;
                                //dr.Close();
                            }
                    }
                }
                conn.Close();
                NpgsqlConnection.ClearAllPools();
                return mje;
            }
            catch (Exception e)
            {
                err = e.Message;
                NpgsqlConnection.ClearAllPools();
                registrar_error("", 2, "carga_sstt_reportes", e.Message);
                return err;
            }
        }


        public static async Task<string> carga_detalleVtaArticulos_newDO()
        {
            string err = "";
            string mje = "";
            int i = 0;

            try
            {
                string lsql;
                NpgsqlConnection conn = new NpgsqlConnection(ETL.Program._cn);
                conn.Open();

                for (i = 0; i < ETL.Program.DetalleVtasDOALL.Count; i++)
                {

                    string ls_pQuantity = Convert.ToString(ETL.Program.DetalleVtasDOALL[i].Quantity).Replace(',', '.');
                    string ls_pPrice = Convert.ToString(ETL.Program.DetalleVtasDOALL[i].Price).Replace(',', '.');
                    string ls_pCosto = Convert.ToString(ETL.Program.DetalleVtasDOALL[i].Costo).Replace(',', '.');
                    string ls_pDescuento = Convert.ToString(ETL.Program.DetalleVtasDOALL[i].Descuento).Replace(',', '.');
                    string ls_pDocRate = Convert.ToString(ETL.Program.DetalleVtasDOALL[i].DocRate).Replace(',', '.');
                    string ls_pCostoLinea = Convert.ToString(ETL.Program.DetalleVtasDOALL[i].CostoLinea).Replace(',', '.');
                    string ls_pDiscPrcnt = Convert.ToString(ETL.Program.DetalleVtasDOALL[i].DiscPrcnt).Replace(',', '.');
                    string ls_pRate = Convert.ToString(ETL.Program.DetalleVtasDOALL[i].Rate).Replace(',', '.');

                    string lsCardName = "";if (ETL.Program.DetalleVtasDOALL[i].CardName is not null)lsCardName = Convert.ToString(ETL.Program.DetalleVtasDOALL[i].CardName).Replace("\'", " ");
                    string lsDscription = "";if (ETL.Program.DetalleVtasDOALL[i].Dscription is not null)lsDscription = Convert.ToString(ETL.Program.DetalleVtasDOALL[i].Dscription).Replace("\'", " ");
                    string lsDespacho_A = "";if (ETL.Program.DetalleVtasDOALL[i].Despacho_A is not null) lsDespacho_A = Convert.ToString(ETL.Program.DetalleVtasDOALL[i].Despacho_A).Replace("\'", " ");
                    string lspJrnlMemo = "";if (ETL.Program.DetalleVtasDOALL[i].JrnlMemo is not null)lspJrnlMemo = Convert.ToString(ETL.Program.DetalleVtasDOALL[i].JrnlMemo).Replace("\'", " ");
                    string lspSalaVentas = ""; if (ETL.Program.DetalleVtasDOALL[i].Sala_de_Ventas is not null) lspSalaVentas = Convert.ToString(ETL.Program.DetalleVtasDOALL[i].Sala_de_Ventas);
                    string lspUCanal = ""; if (ETL.Program.DetalleVtasDOALL[i].U_Canal is not null) lspUCanal = Convert.ToString(ETL.Program.DetalleVtasDOALL[i].U_Canal);
                    string lspUMotivoNC = ""; if (ETL.Program.DetalleVtasDOALL[i].U_MotivoNC is not null) lspUMotivoNC = Convert.ToString(ETL.Program.DetalleVtasDOALL[i].U_MotivoNC);
                    string lspUTipoNC = ""; if (ETL.Program.DetalleVtasDOALL[i].U_TipoNC is not null) lspUTipoNC = Convert.ToString(ETL.Program.DetalleVtasDOALL[i].U_TipoNC);
                    string lspUNCRefacturacion = ""; if (ETL.Program.DetalleVtasDOALL[i].U_NCRefacturacion is not null) lspUNCRefacturacion = Convert.ToString(ETL.Program.DetalleVtasDOALL[i].U_NCRefacturacion);

                    if (ETL.Program.DetalleVtasDOALL[i].FolioNum == 280025)//172131 && pFolioNum <= 172131
                    {
                        Console.WriteLine("revisar-->" + ETL.Program.DetalleVtasDOALL[i].FolioNum);
                    }
                    
                    lsql = "'" + ETL.Program.DetalleVtasDOALL[i].DocDate + "'," +
                            "'" + ETL.Program.DetalleVtasDOALL[i].OcrCode + "'," +
                            "" + ETL.Program.DetalleVtasDOALL[i].GroupCode + "," +
                            "'" + ETL.Program.DetalleVtasDOALL[i].GroupName + "'," +
                            "'" + ETL.Program.DetalleVtasDOALL[i].CardCode + "'," +
                            "'" + lsCardName + "'," +
                            "" + ETL.Program.DetalleVtasDOALL[i].FolioNum + "," +
                            "" + ETL.Program.DetalleVtasDOALL[i].ItmsGrpCod + "," +
                            "'" + ETL.Program.DetalleVtasDOALL[i].ItmsGrpNam + "'," +
                            "'" + ETL.Program.DetalleVtasDOALL[i].Bodega + "'," +
                            "'" + ETL.Program.DetalleVtasDOALL[i].U_Familia + "'," +
                            "'" + ETL.Program.DetalleVtasDOALL[i].ItemCode + "'," +
                            "'" + lsDscription + "'," +
                            "" + ls_pQuantity + "," +
                            "" + ls_pPrice + "," +
                            "" + ls_pCosto + "," +
                            "" + ls_pDescuento + "," +
                            "" + ETL.Program.DetalleVtasDOALL[i].CodVen_Factura + "," +
                            "'" + ETL.Program.DetalleVtasDOALL[i].Vendedor + "'," +
                            "'" + lsDespacho_A + "'," +
                            "'" + ETL.Program.DetalleVtasDOALL[i].Vinculada + "'," +
                            "'" + ETL.Program.DetalleVtasDOALL[i].PymntGroup + "'," +
                            "" + ETL.Program.DetalleVtasDOALL[i].DocNum + "," +
                            "'" + ETL.Program.DetalleVtasDOALL[i].FolioPref + "'," +
                            "'" + ETL.Program.DetalleVtasDOALL[i].OcrCode2 + "'," +
                            "'" + ETL.Program.DetalleVtasDOALL[i].OcrCode3 + "'," +
                            "'" + ETL.Program.DetalleVtasDOALL[i].Empresa + "'," +
                            "'" + ETL.Program.DetalleVtasDOALL[i].DocCur + "'," +
                            "" + ls_pDocRate + "," +
                            "" + ls_pDiscPrcnt + "," +
                            "" + ls_pRate + "," +
                            "" + ls_pCostoLinea + "," +
                            "'" + ETL.Program.DetalleVtasDOALL[i].Activo + "'," +
                            "'" + ETL.Program.DetalleVtasDOALL[i].ID_CRM + "'," +
                            "'" + ETL.Program.DetalleVtasDOALL[i].Tipo_Doc + "'," +
                            "'" + ETL.Program.DetalleVtasDOALL[i].Status + "'," +
                            "'" + ETL.Program.DetalleVtasDOALL[i].CANCELED + "'," +
                            "'" + lspJrnlMemo + "'," +
                            "'" + lspSalaVentas + "',"+
                            "'" + lspUCanal + "'," +
                            "'" + lspUMotivoNC + "'," +
                            "'" + lspUTipoNC + "'," +
                            "'" + lspUNCRefacturacion + "'"
                            ;


                    /*fichero;
                    fichero = File.AppendText("C:\\Users\\carlos.caceres\\OneDrive - Grupo Vielva\\Escritorio\\Nueva carpeta (8)\\data.txt");
                    fichero.WriteLine("select public.carga_detalleVtaArticulos_newDO (" + lsql + ")");
                    fichero.Close();*/

                    NpgsqlConnection.ClearAllPools();
                    await using (NpgsqlCommand cmd = new NpgsqlCommand("select public.fx_carga_detalle_venta_artic_newDO (" + lsql + ")", conn))
                    {
                        await using (NpgsqlDataReader dr = await cmd.ExecuteReaderAsync())
                            while (await dr.ReadAsync())
                            {
                                mje = Convert.ToString(dr[0]);
                                ETL.Program.myContador = ETL.Program.myContador + 1;
                                //dr.Close();
                            }
                    }
                }

                conn.Close();
                NpgsqlConnection.ClearAllPools();
                return mje;
            }
            catch (Exception e)
            {
                err = e.Message;
                NpgsqlConnection.ClearAllPools();
                registrar_error("", 2, "carga_detalleVtaArticulos_newDO", e.Message);
                //log = await ingresa_ERROR_carga(pEmpresa,2, "carga_detalleVtaArticulos", e.Message);

                return err;
            }
        }

        public static async Task<string> carga_oc_all_status()
        {
            string err = "";
            string mje = "";

            try
            {
                string lsql;
                NpgsqlConnection conn = new NpgsqlConnection(ETL.Program._cn);
                conn.Open();

                for (int i = 0; i < ETL.Program.OC_StatusALL.Count; i++)
                {
                    string ls_pQuantity = Convert.ToString(ETL.Program.OC_StatusALL[i].Quantity).Replace(',', '.');
                    string ls_pCan_Recibida = Convert.ToString(ETL.Program.OC_StatusALL[i].Can_Recibida).Replace(',', '.');
                    string ls_pCant_Abierta = Convert.ToString(ETL.Program.OC_StatusALL[i].Cant_Abierta).Replace(',', '.');
                    string ls_pPrice = Convert.ToString(ETL.Program.OC_StatusALL[i].Price).Replace(',', '.');
                    string ls_pTotal = Convert.ToString(ETL.Program.OC_StatusALL[i].Total).Replace(',', '.');
                    string ls_pOpenSum = Convert.ToString(ETL.Program.OC_StatusALL[i].OpenSum).Replace(',', '.');
                    string ls_pQuantitys = Convert.ToString(ETL.Program.OC_StatusALL[i].Quantitys).Replace(',', '.');
                    string lsCardName = "";if (ETL.Program.OC_StatusALL[i].CardName is not null){lsCardName = Convert.ToString(ETL.Program.OC_StatusALL[i].CardName).Replace("\'", " ");}
                    string lsDscription= Convert.ToString(ETL.Program.OC_StatusALL[i].Dscription).Replace("\'", " ");

                    lsql = "'" + ETL.Program.OC_StatusALL[i].CardCode + "'," +
                            "'" + lsCardName + "'," +
                            "" + ETL.Program.OC_StatusALL[i].DocNum + "," +
                            "'" + ETL.Program.OC_StatusALL[i].DocDate + "'," +
                            "'" + ETL.Program.OC_StatusALL[i].ItemCode + "'," +
                            "'" + lsDscription + "'," +
                            "'" + ETL.Program.OC_StatusALL[i].Currency + "'," +
                            "" + ls_pQuantity + "," +
                            "" + ls_pCan_Recibida + "," +
                            "" + ls_pCant_Abierta + "," +
                            "" + ls_pPrice + "," +
                            "" + ls_pTotal + "," +
                            "" + ls_pOpenSum + "," +
                            "'" + ETL.Program.OC_StatusALL[i].WhsCode + "'," +
                            "'" + ETL.Program.OC_StatusALL[i].TaxCode + "'," +
                            "" + ls_pQuantitys + "," +
                            "'" + ETL.Program.OC_StatusALL[i].Comentario + "'," +
                            "'" + ETL.Program.OC_StatusALL[i].U_NAME + "'," +
                            "'" + ETL.Program.OC_StatusALL[i].CANCELED + "'," +
                            "'" + ETL.Program.OC_StatusALL[i].DocStatus + "'," +
                            "'" + ETL.Program.OC_StatusALL[i].DocType + "'," +
                            "'" + ETL.Program.OC_StatusALL[i].BaseRef + "'," +
                            "'" + ETL.Program.OC_StatusALL[i].LineNum + "'," +
                            "'" + ETL.Program.OC_StatusALL[i].LineStatus + "'," +
                            "'" + ETL.Program.OC_StatusALL[i].Fecha_ETD + "'," +
                            "'" + ETL.Program.OC_StatusALL[i].Fecha_ETA + "'," +
                            "'" + ETL.Program.OC_StatusALL[i].NumAtCard + "'," +
                            "'" + ETL.Program.OC_StatusALL[i].Empresa + "'";

                    NpgsqlConnection.ClearAllPools();

                    /*StreamWriter fichero;
                    fichero = File.AppendText("C:\\Users\\carlos.caceres\\OneDrive - Grupo Vielva\\Escritorio\\Nueva carpeta (8)\\data.txt");
                    fichero.WriteLine("select public.fx_carga_oc_all_status (" + lsql + ")");
                    fichero.Close();*/

                    await using (NpgsqlCommand cmd = new NpgsqlCommand("select public.fx_carga_oc_all_status (" + lsql + ")", conn))
                    {
                        await using (NpgsqlDataReader dr = await cmd.ExecuteReaderAsync())
                            while (await dr.ReadAsync())
                            {
                                mje = Convert.ToString(dr[0]);
                                ETL.Program.myContador = ETL.Program.myContador + 1;
                            }
                    }

                }


                conn.Close();
                NpgsqlConnection.ClearAllPools();
                return mje;
            }
            catch (Exception e)
            {
                err = e.Message;
                NpgsqlConnection.ClearAllPools();
                registrar_error("", 2, "carga_oc_all_status", e.Message);
                return err;
            }
        }


        public static async Task<string> carga_tarjetas()
        {
            string err = "";
            string mje = "";

            try
            {   
                string lsql;
                NpgsqlConnection conn = new NpgsqlConnection(ETL.Program._cn);
                conn.Open();

                for (int i = 0; i < ETL.Program.TarjetasALL.Count; i++)
                {
                            lsql = "'" + ETL.Program.TarjetasALL[i].itemCode + "'," +
                            "'" + ETL.Program.TarjetasALL[i].U_Kit_Minero  + "'," +
                            "'" + ETL.Program.TarjetasALL[i].U_Acti_Vinc + "'," +
                            "'" + ETL.Program.TarjetasALL[i].status + "'," +
                             "'" + ETL.Program.TarjetasALL[i].internalSN  + "'," +
                             "" + ETL.Program.TarjetasALL[i].ItmsGrpCod  + "" 
                             ;

                    /*StreamWriter fichero;
                    fichero = File.AppendText("C:\\Users\\carlos.caceres\\OneDrive - Grupo Vielva\\Escritorio\\Nueva carpeta (8)\\data.txt");
                    fichero.WriteLine("select public.fxf_tmp_carga_vendedores (" + lsql + ")");
                    fichero.Close();
                    */

                    NpgsqlConnection.ClearAllPools();
                    await using (NpgsqlCommand cmd = new NpgsqlCommand("select public.fxf_carga_tarjetas (" + lsql + ")", conn))
                    {
                        await using (NpgsqlDataReader dr = await cmd.ExecuteReaderAsync())
                            while (await dr.ReadAsync())
                            {
                                mje = Convert.ToString(dr[0]);
                                ETL.Program.myContador = ETL.Program.myContador + 1;
                                //dr.Close();
                            }
                    }
                }

                conn.Close();
                NpgsqlConnection.ClearAllPools();
                return mje;
            }
            catch (Exception e)
            {
                err = e.Message;
                NpgsqlConnection.ClearAllPools();
                registrar_error("", 2, "carga_tarjetas", e.Message);
                return err;
            }

        }

        public static async Task<string> carga_NombresPropiedades()
        {
            string err = "";
            string mje = "";

            try
            {
                string lsql;
                NpgsqlConnection conn = new NpgsqlConnection(ETL.Program._cn);
                conn.Open();

                for (int i = 0; i < ETL.Program.NombresPropiedadesALL.Count; i++)
                {
                    lsql = ETL.Program.NombresPropiedadesALL[i].ItmsTypCod + ","+
                        "'" + ETL.Program.NombresPropiedadesALL[i].ItmsGrpNam + "'";
                    
                    /*StreamWriter fichero;
                    fichero = File.AppendText("C:\\temp\\ERR\\data.txt");
                    fichero.WriteLine("select public.fxf_tmp_carga_nombrespropiedades (" + lsql + ")");
                    fichero.Close();
                    */
                    NpgsqlConnection.ClearAllPools();
                    await using (NpgsqlCommand cmd = new NpgsqlCommand("select public.fxf_tmp_carga_nombrespropiedades (" + lsql + ")", conn))
                    {
                        await using (NpgsqlDataReader dr = await cmd.ExecuteReaderAsync())
                            while (await dr.ReadAsync())
                            {
                                mje = Convert.ToString(dr[0]);
                                ETL.Program.myContador = ETL.Program.myContador + 1;
                                //dr.Close();
                            }
                    }
                }

                conn.Close();
                NpgsqlConnection.ClearAllPools();
                return mje;
            }
            catch (Exception e)
            {
                err = e.Message;
                NpgsqlConnection.ClearAllPools();
                registrar_error("", 2, "carga_NombresPropiedades", e.Message);
                return err;
            }

        }


        public static async Task<string> carga_EntradaMercaderia()
        {
            string err = "";
            string mje = "";

            try
            {
                string lsql;
                NpgsqlConnection conn = new NpgsqlConnection(ETL.Program._cn);
                conn.Open();

                for (int i = 0; i < ETL.Program.EntradaMercaderiaALL.Count; i++)
                {
                    if (string.IsNullOrEmpty(ETL.Program.EntradaMercaderiaALL[i].CANCELED))
                    {
                        ETL.Program.EntradaMercaderiaALL[i].CANCELED = " ";
                    }
                    if (string.IsNullOrEmpty(ETL.Program.EntradaMercaderiaALL[i].NumAtCard)) {
                        ETL.Program.EntradaMercaderiaALL[i].NumAtCard = " ";
                    }
                    if (string.IsNullOrEmpty(ETL.Program.EntradaMercaderiaALL[i].CardCode))
                    {
                        ETL.Program.EntradaMercaderiaALL[i].CardCode = " ";
                    }
                    string CardName=Convert.ToString(ETL.Program.EntradaMercaderiaALL[i].CardName).Replace("\'", " ");
                    string Dscription = Convert.ToString(ETL.Program.EntradaMercaderiaALL[i].Dscription).Replace("\'", " ");
                    lsql = ETL.Program.EntradaMercaderiaALL[i].DocNum + "," +
                        "'" + ETL.Program.EntradaMercaderiaALL[i].CANCELED + "'" + "," +
                        "'" + ETL.Program.EntradaMercaderiaALL[i].DocStatus + "'" + "," +
                        "'" + ETL.Program.EntradaMercaderiaALL[i].DocDate + "'" + "," +
                        "'" + ETL.Program.EntradaMercaderiaALL[i].CardCode + "'" + "," +
                        "'" + CardName + "'" + "," +
                        "'" + ETL.Program.EntradaMercaderiaALL[i].NumAtCard + "'" + "," +
                            + ETL.Program.EntradaMercaderiaALL[i].DocRate + "," +
                            + ETL.Program.EntradaMercaderiaALL[i].SlpCode + "," +
                            + ETL.Program.EntradaMercaderiaALL[i].LineNum + "," +
                        "'" + ETL.Program.EntradaMercaderiaALL[i].BaseRef + "'" + "," +
                            + ETL.Program.EntradaMercaderiaALL[i].BaseType + "," +
                        "'" + ETL.Program.EntradaMercaderiaALL[i].LineStatus + "'" + "," +
                        "'" + ETL.Program.EntradaMercaderiaALL[i].ItemCode + "'" + "," +
                        "'" + Dscription + "'" + "," +
                            + ETL.Program.EntradaMercaderiaALL[i].Quantity + "," +
                            + ETL.Program.EntradaMercaderiaALL[i].OpenQty + "," +
                            + ETL.Program.EntradaMercaderiaALL[i].Price + "," +
                        "'" + ETL.Program.EntradaMercaderiaALL[i].Currency + "'" + "," +
                            + ETL.Program.EntradaMercaderiaALL[i].Rate + "," +
                            + ETL.Program.EntradaMercaderiaALL[i].LineTotal + "," +
                        "'" + ETL.Program.EntradaMercaderiaALL[i].OcrCode+ "'" ;

                    StreamWriter fichero;
                    fichero = File.AppendText("C:\\temp\\ERR\\data.txt");
                    fichero.WriteLine("select public.fxf_carga_entradamercaderia (" + lsql + ")");
                    fichero.Close();
                    
                    NpgsqlConnection.ClearAllPools();
                    await using (NpgsqlCommand cmd = new NpgsqlCommand("select public.fxf_carga_entradamercaderia (" + lsql + ")", conn))
                    {
                        await using (NpgsqlDataReader dr = await cmd.ExecuteReaderAsync())
                            while (await dr.ReadAsync())
                            {
                                mje = Convert.ToString(dr[0]);
                                ETL.Program.myContador = ETL.Program.myContador + 1;
                                //dr.Close();
                            }
                    }
                }

                conn.Close();
                NpgsqlConnection.ClearAllPools();
                return mje;
            }
            catch (Exception e)
            {
                err = e.Message;
                NpgsqlConnection.ClearAllPools();
                registrar_error("", 2, "carga_EntradaMercaderia", e.Message);
                return err;
            }

        }


        public static async Task<string> carga_PropiedadesProductos()
        {
            string err = "";
            string mje = "";

            try
            {
                string lsql;
                NpgsqlConnection conn = new NpgsqlConnection(ETL.Program._cn);
                conn.Open();

                for (int i = 0; i < ETL.Program.PropiedadesProductosALL.Count; i++)
                {
                    lsql = "'" + ETL.Program.PropiedadesProductosALL[i].ItemCode + "'," +
                        "'" + ETL.Program.PropiedadesProductosALL[i].QryGroup1 + "'" + "," +
                        "'" + ETL.Program.PropiedadesProductosALL[i].QryGroup2 + "'" + "," +
                        "'" + ETL.Program.PropiedadesProductosALL[i].QryGroup4 + "'" + "," +
                        "'" + ETL.Program.PropiedadesProductosALL[i].QryGroup3 + "'" + "," +
                        "'" + ETL.Program.PropiedadesProductosALL[i].QryGroup5 + "'" + "," +
                        "'" + ETL.Program.PropiedadesProductosALL[i].QryGroup6 + "'" + "," +
                        "'" + ETL.Program.PropiedadesProductosALL[i].QryGroup7 + "'" + "," +
                        "'" + ETL.Program.PropiedadesProductosALL[i].QryGroup8 + "'" + "," +
                        "'" + ETL.Program.PropiedadesProductosALL[i].QryGroup9 + "'" + "," +
                        "'" + ETL.Program.PropiedadesProductosALL[i].QryGroup10 + "'" + "," +
                        "'" + ETL.Program.PropiedadesProductosALL[i].QryGroup11 + "'" + "," +
                        "'" + ETL.Program.PropiedadesProductosALL[i].QryGroup12 + "'" + "," +
                        "'" + ETL.Program.PropiedadesProductosALL[i].QryGroup13 + "'" + "," +
                        "'" + ETL.Program.PropiedadesProductosALL[i].QryGroup14 + "'" + "," +
                        "'" + ETL.Program.PropiedadesProductosALL[i].QryGroup15 + "'" + "," +
                        "'" + ETL.Program.PropiedadesProductosALL[i].QryGroup16 + "'" + "," +
                        "'" + ETL.Program.PropiedadesProductosALL[i].QryGroup17 + "'" + "," +
                        "'" + ETL.Program.PropiedadesProductosALL[i].QryGroup18 + "'";

                    /*
                    StreamWriter fichero;
                    fichero = File.AppendText("C:\\temp\\ERR\\data.txt");
                    fichero.WriteLine("select public.fxf_carga_propiedadesproductos (" + lsql + ")");
                    fichero.Close();
                    */
                    NpgsqlConnection.ClearAllPools();
                    await using (NpgsqlCommand cmd = new NpgsqlCommand("select public.fxf_carga_propiedadesproductos (" + lsql + ")", conn))
                    {
                        await using (NpgsqlDataReader dr = await cmd.ExecuteReaderAsync())
                            while (await dr.ReadAsync())
                            {
                                mje = Convert.ToString(dr[0]);
                                ETL.Program.myContador = ETL.Program.myContador + 1;
                                //dr.Close();
                            }
                    }
                }

                conn.Close();
                NpgsqlConnection.ClearAllPools();
                return mje;
            }
            catch (Exception e)
            {
                err = e.Message;
                NpgsqlConnection.ClearAllPools();
                registrar_error("", 2, "carga_PropiedadesProductos", e.Message);
                return err;
            }

        }

        public static async Task<string> carga_Pallets()
        {
            string err = "";
            string mje = "";

            try
            {
                string lsql;
                NpgsqlConnection conn = new NpgsqlConnection(ETL.Program._cn);
                conn.Open();

                for (int i = 0; i < ETL.Program.PalletsALL.Count; i++)
                {

                    string lsItemName = Convert.ToString(ETL.Program.PalletsALL[i].ItemName).Replace("\'", " ");
                    string ls_pU_NUniXpallet = Convert.ToString(ETL.Program.PalletsALL[i].U_NUniXpallet).Replace(',', '.');
                    string ls_pPallets_en_Ubic = Convert.ToString(ETL.Program.PalletsALL[i].Pallets_en_Ubic).Replace(',', '.');
                    lsql = "'" + ETL.Program.PalletsALL[i].ItemCode + "'," +
                    "'" + lsItemName + "'," +
                    "'" + ETL.Program.PalletsALL[i].U_Nivel_Maximo + "'," +
                    "'" + ETL.Program.PalletsALL[i].BinCode + "'," +
                    "'" + ETL.Program.PalletsALL[i].Ubicacion + "'," +
                    "'" + ETL.Program.PalletsALL[i].Nivel_Actual + "'," +
                    "'" + ETL.Program.PalletsALL[i].WhsCode + "'," +
                    "" + ETL.Program.PalletsALL[i].Cant_en_Ubic + "," +
                    "" + ls_pU_NUniXpallet + "," +
                    "" + ls_pPallets_en_Ubic + "";


                    /*
                    StreamWriter fichero;
                    fichero = File.AppendText("C:\\temp\\ERR\\data.txt");
                    fichero.WriteLine("select public.fxf_tmp_carga_pallets (" + lsql + ")");
                    fichero.Close();
                    */

                    NpgsqlConnection.ClearAllPools();


                    await using (NpgsqlCommand cmd = new NpgsqlCommand("select public.fxf_carga_pallets (" + lsql + ")", conn))
                    {
                        await using (NpgsqlDataReader dr = await cmd.ExecuteReaderAsync())
                            while (await dr.ReadAsync())
                            {
                                mje = Convert.ToString(dr[0]);
                                ETL.Program.myContador = ETL.Program.myContador + 1;
                                //dr.Close();
                            }
                    }
                }

                conn.Close();
                NpgsqlConnection.ClearAllPools();
                return mje;
            }
            catch (Exception e)
            {
                err = e.Message;
                NpgsqlConnection.ClearAllPools();
                registrar_error("", 2, "carga_pallets", e.Message);
                return err;
            }

        }

        public static async Task<string> carga_Articulos()
        {
            string err = "";
            string mje = "";

            try
            {
                string lsql;
                NpgsqlConnection conn = new NpgsqlConnection(ETL.Program._cn);
                conn.Open();

                for (int i = 0; i < ETL.Program.ArticulosALL.Count; i++)
                {
                    string lsItemName = Convert.ToString(ETL.Program.ArticulosALL[i].ItemName).Replace("\'", " ");
                    string lsItemName2 = Convert.ToString(ETL.Program.ArticulosALL[i].ItmsGrpNam).Replace("\'", " ");
                    string ls_pSHeight1 = Convert.ToString(ETL.Program.ArticulosALL[i].SHeight1).Replace(',', '.');
                    string ls_pSWdth1Unit = Convert.ToString(ETL.Program.ArticulosALL[i].SWdth1Unit).Replace(',', '.');
                    string ls_pSLength1 = Convert.ToString(ETL.Program.ArticulosALL[i].SLength1).Replace(',', '.');
                    string ls_pVolumen_vta = Convert.ToString(ETL.Program.ArticulosALL[i].Volumen_vta).Replace(',', '.');
                    string ls_pSWeight1 = Convert.ToString(ETL.Program.ArticulosALL[i].SWeight1).Replace(',', '.');
                    string ls_pBHeight1 = Convert.ToString(ETL.Program.ArticulosALL[i].BHeight1).Replace(',', '.');
                    string ls_pBWidth1 = Convert.ToString(ETL.Program.ArticulosALL[i].BWidth1).Replace(',', '.');
                    string ls_pBLength1 = Convert.ToString(ETL.Program.ArticulosALL[i].BLength1).Replace(',', '.');
                    string ls_pBVolume = Convert.ToString(ETL.Program.ArticulosALL[i].BVolume).Replace(',', '.');
                    string ls_pBWeight1 = Convert.ToString(ETL.Program.ArticulosALL[i].BWeight1).Replace(',', '.');

                    lsql = "'" + ETL.Program.ArticulosALL[i].ItemCode + "'," +
                    "'" + lsItemName + "'," +
                    "'" + lsItemName2 + "'," +
                    "'" + ETL.Program.ArticulosALL[i].CodeBars + "'," +
                     "" + ETL.Program.ArticulosALL[i].OnHand + "," +
                     "'" + ETL.Program.ArticulosALL[i].CardCode + "'," +
                     "" + ls_pSHeight1 + "," +
                     "" + ls_pSWdth1Unit + "," +
                    "" + ls_pSLength1 + "," +
                    "" + ls_pVolumen_vta + "," +
                    "" + ls_pSWeight1 + "," +
                     "" + ls_pBHeight1 + "," +
                     "" + ls_pBWidth1 + "," +
                     "" + ls_pBLength1 + "," +
                     "" + ls_pBVolume + "," +
                     "" + ls_pBWeight1 + "," +
                     "'" + ETL.Program.ArticulosALL[i].U_Nivel_Maximo + "'," +
                     "" + ETL.Program.ArticulosALL[i].U_NUniXpallet + "";

                    /*
                    StreamWriter fichero;
                    fichero = File.AppendText("C:\\temp\\ERR\\data.txt");
                    fichero.WriteLine("select public.fxf_tmp_carga_articulos (" + lsql + ")");
                    fichero.Close();
                    */

                    /*StreamWriter fichero;
                    fichero = File.AppendText("C:\\Users\\carlos.caceres\\OneDrive - Grupo Vielva\\Escritorio\\Nueva carpeta (8)\\data.txt");
                    fichero.WriteLine("select public.fxf_tmp_carga_vendedores (" + lsql + ")");
                    fichero.Close();
                    */

                    NpgsqlConnection.ClearAllPools();


                    await using (NpgsqlCommand cmd = new NpgsqlCommand("select public.fxf_carga_articulos (" + lsql + ")", conn))
                    {
                        await using (NpgsqlDataReader dr = await cmd.ExecuteReaderAsync())
                            while (await dr.ReadAsync())
                            {
                                mje = Convert.ToString(dr[0]);
                                ETL.Program.myContador = ETL.Program.myContador + 1;
                                //dr.Close();
                            }
                    }
                }

                conn.Close();
                NpgsqlConnection.ClearAllPools();
                return mje;
            }
            catch (Exception e)
            {
                err = e.Message;
                NpgsqlConnection.ClearAllPools();
                registrar_error("", 2, "carga_articulos", e.Message);
                return err;
            }

        }

        public static async Task<string> carga_Vendedores()
        {
            string err = "";
            string mje = "";

            int pSlpCode; string pSlpName; string pMemo; string pActive; 
            string pU_Equipo; string pU_VendedorComision; string pU_Sucursal; string pU_Area;string pU_Gerente; string pU_GruArtPer; string pU_Tipo; int pCNT; string pempresa;
            string pU_Autorizador;
            try
            {
                string lsql;
                NpgsqlConnection conn = new NpgsqlConnection(ETL.Program._cn);
                conn.Open();

                for (int i = 0; i < ETL.Program.VendedorALL.Count; i++)
                {
                    pSlpCode = ETL.Program.VendedorALL[i].SlpCode;
                    pSlpName = ETL.Program.VendedorALL[i].SlpName;
                    pMemo = ETL.Program.VendedorALL[i].Memo ;
                    pActive = ETL.Program.VendedorALL[i].Active ;
                    pU_Equipo = ETL.Program.VendedorALL[i].U_Equipo;
                    pU_VendedorComision = ETL.Program.VendedorALL[i].U_VendedorComision ;
                    pU_Sucursal = ETL.Program.VendedorALL[i].U_Sucursal ;
                    pU_Area = ETL.Program.VendedorALL[i].U_Area;
                    pU_Gerente = ETL.Program.VendedorALL[i].U_Gerente;
                    pU_GruArtPer = ETL.Program.VendedorALL[i].U_GruArtPer;
                    pU_Tipo = ETL.Program.VendedorALL[i].U_Tipo;
                    pCNT = ETL.Program.VendedorALL[i].CNT;
                    pU_Autorizador = ETL.Program.VendedorALL[i].U_Autorizador;
                     pempresa = ETL.Program.VendedorALL[i].Empresa ;

                    lsql = "'" + pSlpCode + "'," +
                           "'" + pSlpName + "'," +
                           "'" + pMemo + "'," +
                           "'" + pActive + "'," +
                            "'" + pU_Equipo + "'," +
                            "'" + pU_VendedorComision + "'," +
                            "'" + pU_Sucursal + "'," +
                            "'" + pU_Area + "'," +
                            "'" + pU_Gerente + "'," +
                            "'" + pU_GruArtPer + "'," +
                            "'" + pU_Tipo + "'," +
                            "'" + pU_Autorizador + "'," +
                            "'" + pempresa + "'";


                    /*StreamWriter fichero;
                    fichero = File.AppendText("C:\\Users\\carlos.caceres\\OneDrive - Grupo Vielva\\Escritorio\\Nueva carpeta (8)\\data.txt");
                    fichero.WriteLine("select public.fxf_tmp_carga_vendedores (" + lsql + ")");
                    fichero.Close();
                    */

                    NpgsqlConnection.ClearAllPools();
                    

                    await using (NpgsqlCommand cmd = new NpgsqlCommand("select public.fxf_tmp_carga_vendedores (" + lsql + ")", conn))
                    {
                        await using (NpgsqlDataReader dr = await cmd.ExecuteReaderAsync())
                            while (await dr.ReadAsync())
                            {
                                mje = Convert.ToString(dr[0]);
                                ETL.Program.myContador = ETL.Program.myContador + 1;
                                //dr.Close();
                            }
                    }
                }

                conn.Close();
                NpgsqlConnection.ClearAllPools();
                return mje;
            }
            catch (Exception e)
            {
                err = e.Message;
                NpgsqlConnection.ClearAllPools();
                registrar_error("", 2, "carga_Vendedores", e.Message);
                return err;
            }

        }
       
       
    }   
}
