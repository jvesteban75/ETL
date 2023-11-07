// ETL FINAL VERSION
// DEVELOPED BY CARLOS CACERES
// MODIFIED BY JORGE VILLAVICENCIO
// LAST MODIFICATION DATE: 2023-11-07



using ETL.Model;
using Newtonsoft.Json;
using Npgsql; //Npgsql .NET Data Provider for PostgreSQL
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using System.IO;


namespace ETL
{
    class Program
    {
        public static string _sl = "https://119.8.155.113:50000/b1s/v1/";
        public static string _cn = "Server=192.168.1.27;User Id=postgres;Password=D4Ta8As3.r0Ot;Database=VIELVA_FUSION;";
        public static string FD6, FH6, FD7, FH7, FD22, FH22;
        public static string FD8, FH8; 
        public static DateTime FII_F, FFF_F, FII_FResp, FFF_FResp;
        public static List<Vendedor> VendedorALL = new List<Vendedor>();
        public static List<LibroMayor> LibroMayorALL = new List<LibroMayor>();
        public static List<SocNegocio> SocNegocioALL = new List<SocNegocio>();
        public static List<MaestoBodega> MaestoBodegaALL = new List<MaestoBodega>();
        public static List<Productos> ProductosALL = new List<Productos>();
        public static List<ListaPrecio> ListaPrecioALL = new List<ListaPrecio>();
        public static List<StockProducto> StockProductoALL = new List<StockProducto>();
        public static List<OC_Status> OC_StatusALL = new List<OC_Status>();
        public static List<DetalleVtasDO> DetalleVtasDOALL = new List<DetalleVtasDO>();
        public static List<SSTTrep> SSTTrepALL = new List<SSTTrep>();
        public static List<NVtaDig> NVtaDigALL = new List<NVtaDig>();
        public static List<PedidoBloq> PedidoBloqALL = new List<PedidoBloq>();
        public static List<PartidasAbiertas> PartidasAbiertasALL = new List<PartidasAbiertas>();
        public static List<FacturaReserv> FacturaReservALL = new List<FacturaReserv>();
        public static List<OfertaVta> OfertaVtaALL = new List<OfertaVta>();
        public static List<GuiaFactura> GuiaFacturaALL = new List<GuiaFactura>();
        public static List<Tarjetas> TarjetasALL = new List<Tarjetas>();
        public static List<Pallets> PalletsALL = new List<Pallets>();
        public static List<Articulos> ArticulosALL = new List<Articulos>();
        public static List<NombresPropiedades> NombresPropiedadesALL = new List<NombresPropiedades>();
        public static List<EntradaMercaderia> EntradaMercaderiaALL = new List<EntradaMercaderia>();
        public static List<PropiedadesProductos> PropiedadesProductosALL = new List<PropiedadesProductos>();


        //public static List<string> sociedades = new List<string> { "PERU", "VIELCO", "VIELVA", "TEST" };
        public static List<string> sociedades = new List<string> { "VIELVA" };

        public static string session = "";
        public static int myContador = 0;
        public static int myErr = 0;
        public static int tiSkip = 0;

        static async Task Main(string[] args)
        {
            string err = "";
            string rta = "";
            try
            {
                string hoy = DateTime.Now.ToString("MMMM yy");
                Console.WriteLine("          ");
                Console.WriteLine("=======================================================");
                Console.WriteLine("******     VIELVA FUSION 2023-SEPT-15 "+hoy+"      *****************");
                Console.WriteLine("=======================================================");
                Console.WriteLine("          ");
                
                //LOGIN SAP
                session = await LoginSAP();

                string datetime_ini = DateTime.Now.ToString("hh:mm:ss tt");
                if (session=="" || session is null)
                {
                    Console.WriteLine(".... Sin Conexion a SAP : " + datetime_ini + " - " + datetime_ini);
                    PBI.clsMaestroProductos.registrar_error("TODAS", 1, "MAIN", "SIN CONEXION A SAP");
                }
                else
                { 
                    //=================================================
                    // CAMBIAR PARA VARIAR RANGO DE FECHA
                    //=================================================
                    rta = await PBI.clsMaestroProductos.obtener_rango_fechas();                 //3 MESES

                    string datetime_fin = DateTime.Now.ToString("hh:mm:ss tt");
                    Console.WriteLine(".... Conexion a SAP : " + datetime_ini + " - " + datetime_fin);

                    Console.WriteLine("          ");
                    Console.WriteLine("          ");
                    Console.WriteLine("============================================================= ");
                    Console.WriteLine(".... 01.-CARGAS SIN FECHAS: " + rta);
                    rta = await cargas_sin_fechas();

                    Console.WriteLine("          ");
                    Console.WriteLine("          ");
                    Console.WriteLine("============================================================= ");
                    Console.WriteLine(".... 02.-  CARGAS CON FECHAS: " + rta);
                    Console.WriteLine("============================================================= ");
                    rta = await cargas_con_fechas();

                                        
                    Console.WriteLine("          ");
                    Console.WriteLine("          ");
                    Console.WriteLine("============================================================= ");
                    Console.WriteLine(".... fin : " + rta);
                }
                    //rta = await PBI.clsMaestroProductos.correo();
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error("TODAS", 1, "MAIN", e.Message);
                Console.WriteLine("ERROR -->" + err);
            }
        }


        public static async Task<string> cargas_sin_fechas()
        {
            string err = "";
            int regHANA = 0;
            string reg_update = "";
            int regPostgrest = 0;
            try
            {
                string datetime_ini = DateTime.Now.ToString("hh:mm:ss tt");
                string datetime_fin = DateTime.Now.ToString("hh:mm:ss tt");

                //==============================================================================
                //                    SIN FECHAS 
                //==============================================================================

                //          AREA DE PRUEBAS. INSERTE AQUI EL CODIGO A PROBAR





                //          FIN DEL AREA DE PRUEBAS



                //10001     fvi_maestro_vendedores
                regHANA = 0;
                datetime_ini = DateTime.Now.ToString("hh:mm:ss tt");
                foreach (var sociedad in sociedades)
                {
                    regHANA = await SAP_vendedor(sociedad);
                }
                if (VendedorALL.Count > 0)
                {
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10001, 1);//1=ELIMINA TABLA TMP
                    reg_update = await PBI.clsMaestroProductos.carga_Vendedores();
                    datetime_fin = DateTime.Now.ToString("hh:mm:ss tt");
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10001, 2);//2=CARGA TABLA PRD
                    reg_update = await PBI.clsMaestroProductos.carga_data_update("fvi_maestro_vendedores", VendedorALL.Count, DateTime.Now, datetime_ini, datetime_fin);
                    Console.WriteLine("01.- MAESTRO_VENDEDORES : " + datetime_ini + " - " + datetime_fin + " - " + VendedorALL.Count);
                    PBI.clsMaestroProductos.registrar_datos("01.- MAESTRO_VENDEDORES : " + datetime_ini + " - " + datetime_fin + " - " + VendedorALL.Count);
                }
                else
                {
                    Console.WriteLine(".... ERROR 01.- MAESTRO_VENDEDORES SIN DATOS SAP : " + datetime_ini + " - " + datetime_ini);
                    PBI.clsMaestroProductos.registrar_error("TODAS", 1, "01.- MAESTRO_VENDEDORES", "SIN DATOS DESDE SAP");
                }


                //10002     fvi_maestra_soc_neg
                regHANA = 0;
                regPostgrest = 0;
                datetime_ini = DateTime.Now.ToString("hh:mm:ss tt");
                foreach (var sociedad in sociedades)
                {
                    regHANA = await SAP_socios_de_Negocios(sociedad);
                }
                if (SocNegocioALL.Count > 0)
                {
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10002, 1);//1=ELIMINA TABLA TMP
                    reg_update = await PBI.clsMaestroProductos.cargasocios_de_negocios();
                    datetime_fin = DateTime.Now.ToString("hh:mm:ss tt");
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10002, 2);//1=ELIMINA TABLA TMP
                    reg_update = await PBI.clsMaestroProductos.carga_data_update("fvi_maestra_soc_neg", SocNegocioALL.Count, DateTime.Now, datetime_ini, datetime_fin);
                    Console.WriteLine("02.- SOCIO DE NEGOCIOS : " + datetime_ini + " - " + datetime_fin + " - " + SocNegocioALL.Count);
                    PBI.clsMaestroProductos.registrar_datos("02.- SOCIO DE NEGOCIOS : " + datetime_ini + " - " + datetime_fin + " - " + SocNegocioALL.Count);
                }
                else
                {
                    Console.WriteLine(".... ERROR 02.- SOCIO DE NEGOCIOS  SIN DATOS SAP : " + datetime_ini + " - " + datetime_ini);
                    PBI.clsMaestroProductos.registrar_error("TODAS", 1, "SIN FECHAS - 02 SOCIO DE NEGOCIOS", "SIN DATOS DESDE SAP");
                }


                //10003     fvi_mamestro_bodega
                regHANA = 0;
                regPostgrest = 0;
                datetime_ini = DateTime.Now.ToString("hh:mm:ss tt");
                foreach (var sociedad in sociedades)
                {
                    regHANA = await SAP_MAESTRO_BODEGAS(sociedad);
                }
                if (MaestoBodegaALL.Count > 0)
                {
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10003, 1);//1=ELIMINA TABLA TMP
                    reg_update = await PBI.clsMaestroProductos.carga_MAESTRO_BODEGAS();
                    datetime_fin = DateTime.Now.ToString("hh:mm:ss tt");
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10003, 2);//1=ELIMINA TABLA TMP
                    reg_update = await PBI.clsMaestroProductos.carga_data_update("fvi_mamestro_bodega", MaestoBodegaALL.Count, DateTime.Now, datetime_ini, datetime_fin);
                    Console.WriteLine("03.- MAESTRO DE BODEGAS : " + datetime_ini + " - " + datetime_fin + " - " + MaestoBodegaALL.Count);
                    PBI.clsMaestroProductos.registrar_datos("03.- MAESTRO DE BODEGAS : " + datetime_ini + " - " + datetime_fin + " - " + MaestoBodegaALL.Count);
                }
                else
                {
                    Console.WriteLine(".... ERROR 03  MAESTRO DE BODEGA SIN DATOS SAP : " + datetime_ini + " - " + datetime_ini);
                    PBI.clsMaestroProductos.registrar_error("TODAS", 1, "SIN FECHAS - 03 MAESTRO DE BODEGA", "SIN DATOS DESDE SAP");
                }



                //10004     fvi_mamestro_producto
                regHANA = 0;
                regPostgrest = 0;
                datetime_ini = DateTime.Now.ToString("hh:mm:ss tt");
                foreach (var sociedad in sociedades)
                {
                    regHANA = await SAP_productos(sociedad);
                }
                if (ProductosALL.Count > 0)
                {
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10004, 1);//1=ELIMINA TABLA TMP
                    reg_update = await PBI.clsMaestroProductos.cargaPRODUCTOSV200();//carga_PRODUCTOS
                    datetime_fin = DateTime.Now.ToString("hh:mm:ss tt");
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10004, 2);//1=ELIMINA TABLA TMP
                    reg_update = await PBI.clsMaestroProductos.carga_data_update("fvi_mamestro_producto", ProductosALL.Count, DateTime.Now, datetime_ini, datetime_fin);
                    Console.WriteLine("04.- MAESTRO DE PRODUCTOS : " + datetime_ini + " - " + datetime_fin + " - " + ProductosALL.Count);
                    PBI.clsMaestroProductos.registrar_datos("04.- MAESTRO DE PRODUCTOS : " + datetime_ini + " - " + datetime_fin + " - " + ProductosALL.Count);
                }
                else
                {
                    Console.WriteLine(".... ERROR 04 MAESTRO DE PRODUCTOS SIN DATOS SAP : " + datetime_ini + " - " + datetime_ini);
                    PBI.clsMaestroProductos.registrar_error("TODAS", 1, "SIN FECHAS - 04 MAESTRO DE PRODUCTOS", "SIN DATOS DESDE SAP");
                }



                //10005     fvi_lista_precios
                regHANA = 0;
                regPostgrest = 0;
                datetime_ini = DateTime.Now.ToString("hh:mm:ss tt");
                foreach (var sociedad in sociedades)
                {
                    regHANA = await SAP_lista_precios(sociedad);
                }
                if (ListaPrecioALL.Count > 0)
                {
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10005, 1);//1=ELIMINA TABLA TMP
                    reg_update = await PBI.clsMaestroProductos.carga_lista_de_precios();
                    datetime_fin = DateTime.Now.ToString("hh:mm:ss tt");
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10005, 2);//1=ELIMINA TABLA TMP
                    reg_update = await PBI.clsMaestroProductos.carga_data_update("fvi_lista_precios", ListaPrecioALL.Count, DateTime.Now, datetime_ini, datetime_fin);
                    Console.WriteLine("05.- LISTA DE PRECIOS : " + datetime_ini + " - " + datetime_fin + " - " + ListaPrecioALL.Count);
                    PBI.clsMaestroProductos.registrar_datos("05.- LISTA DE PRECIOS : " + datetime_ini + " - " + datetime_fin + " - " + ListaPrecioALL.Count);
                }
                else
                {
                    Console.WriteLine(".... ERROR 05 LISTA DE PRECIOS SIN DATOS SAP : " + datetime_ini + " - " + datetime_ini);
                    PBI.clsMaestroProductos.registrar_error("TODAS", 1, "SIN FECHAS - 05 LISTA DE PRECIOS", "SIN DATOS DESDE SAP");
                }



                //10006     fvi_stock_productos
                regHANA = 0;
                regPostgrest = 0;
                datetime_ini = DateTime.Now.ToString("hh:mm:ss tt");
                foreach (var sociedad in sociedades)
                {
                    regHANA = await SAP_stock_de_productos(sociedad);
                }
                if (StockProductoALL.Count > 0)
                {
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10006, 1);//1=ELIMINA TABLA TMP
                    reg_update = await PBI.clsMaestroProductos.carga_stock_productos();
                    datetime_fin = DateTime.Now.ToString("hh:mm:ss tt");
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10006, 2);//2=ELIMINA TABLA PRD
                    reg_update = await PBI.clsMaestroProductos.carga_data_update("fvi_stock_productos", StockProductoALL.Count, DateTime.Now, datetime_ini, datetime_fin);
                    Console.WriteLine("06.- STOCK DE PRODUCTOS : " + datetime_ini + " - " + datetime_fin + " - " + StockProductoALL.Count);
                    PBI.clsMaestroProductos.registrar_datos("06.- STOCK DE PRODUCTOS : " + datetime_ini + " - " + datetime_fin + " - " + StockProductoALL.Count);
                }
                else
                {
                    Console.WriteLine(".... ERROR 06 STOCK DE PRODUCTOS SIN DATOS SAP : " + datetime_ini + " - " + datetime_ini);
                    PBI.clsMaestroProductos.registrar_error("TODAS", 1, "SIN FECHAS - 06 STOCK DE PRODUCTOS", "SIN DATOS DESDE SAP");
                }



                //10007     fvi_oc_all_status
                regHANA = 0;
                regPostgrest = 0;
                datetime_ini = DateTime.Now.ToString("hh:mm:ss tt");

                //regHANA = await SAP_OC_All_Estatus("TODAS");
                foreach (var sociedad in sociedades)
                {
                    regHANA = await SAP_OC_All_Estatus(sociedad);
                }
                if (OC_StatusALL.Count > 0)
                {
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10007, 1);//1=ELIMINA TABLA TMP / 2= PRD
                    reg_update = await PBI.clsMaestroProductos.carga_oc_all_status();
                    datetime_fin = DateTime.Now.ToString("hh:mm:ss tt");
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10007, 2);//1=ELIMINA TABLA TMP / 2= PRD
                    reg_update = await PBI.clsMaestroProductos.carga_data_update("fvi_oc_all_status", OC_StatusALL.Count, DateTime.Now, datetime_ini, datetime_fin);
                    Console.WriteLine("07.- OC ALL Status : " + datetime_ini + " - " + datetime_fin + " - " + OC_StatusALL.Count);
                    PBI.clsMaestroProductos.registrar_datos("07.- OC ALL Status : " + datetime_ini + " - " + datetime_fin + " - " + OC_StatusALL.Count);
                }
                else
                {
                    Console.WriteLine(".... ERROR 07 OC ALL Status SIN DATOS SAP : " + datetime_ini + " - " + datetime_ini);
                    PBI.clsMaestroProductos.registrar_error("TODAS", 1, "SIN FECHAS - 07 OC ALL Status", "SIN DATOS DESDE SAP");
                }


                //10008     fvi_facturas_reservas
                regHANA = 0;
                regPostgrest = 0;
                datetime_ini = DateTime.Now.ToString("hh:mm:ss tt");
                foreach (var sociedad in sociedades)
                {
                    regHANA = await SAP_factura_reserva(sociedad);
                }
                if (FacturaReservALL.Count > 0)
                {
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10008, 1);//1=ELIMINA TABLA TMP / 2= PRD
                    reg_update = await PBI.clsMaestroProductos.carga_factura_reserva();
                    datetime_fin = DateTime.Now.ToString("hh:mm:ss tt");
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10008, 2);//1=ELIMINA TABLA TMP / 2= PRD
                    reg_update = await PBI.clsMaestroProductos.carga_data_update("fvi_facturas_reservas", FacturaReservALL.Count, DateTime.Now, datetime_ini, datetime_fin);
                    Console.WriteLine("08.- FACTURA DE RESERVAS : " + datetime_ini + " - " + datetime_fin + " - " + FacturaReservALL.Count);
                    PBI.clsMaestroProductos.registrar_datos("08.- FACTURA DE RESERVAS : " + datetime_ini + " - " + datetime_fin + " - " + FacturaReservALL.Count);
                }
                else
                {
                    Console.WriteLine(".... ERROR 08 FACTURA DE RESERVAS SIN DATOS SAP : " + datetime_ini + " - " + datetime_ini);
                    PBI.clsMaestroProductos.registrar_error("TODAS", 1, "SIN FECHAS - 08 FACTURA DE RESERVAS", "SIN DATOS DESDE SAP");
                }



                //10009     fvi_partidas_abiertas
                regHANA = 0;
                regPostgrest = 0;
                datetime_ini = DateTime.Now.ToString("hh:mm:ss tt");
                foreach (var sociedad in sociedades)
                {
                    regHANA = await SAP_patidasAbiertas(sociedad);
                }
                if (PartidasAbiertasALL.Count > 0)
                {
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10009, 1);//1=ELIMINA TABLA TMP / 2= PRD
                    reg_update = await PBI.clsMaestroProductos.cargaPartidasAbiertas();
                    datetime_fin = DateTime.Now.ToString("hh:mm:ss tt");
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10009, 2);//1=ELIMINA TABLA TMP / 2= PRD
                    reg_update = await PBI.clsMaestroProductos.carga_data_update("fvi_partidas_abiertas", PartidasAbiertasALL.Count, DateTime.Now, datetime_ini, datetime_fin);
                    Console.WriteLine("09.- PARTIDAS ABIERTAS : " + datetime_ini + " - " + datetime_fin + " - " + PartidasAbiertasALL.Count);
                    PBI.clsMaestroProductos.registrar_datos("09.- PARTIDAS ABIERTAS : " + datetime_ini + " - " + datetime_fin + " - " + PartidasAbiertasALL.Count);
                }
                else
                {
                    Console.WriteLine(".... ERROR 09 PARTIDAS ABIERTAS SIN DATOS SAP : " + datetime_ini + " - " + datetime_ini);
                    PBI.clsMaestroProductos.registrar_error("TODAS", 1, "SIN FECHAS - 09 PARTIDAS ABIERTAS", "SIN DATOS DESDE SAP");
                }




                //10010     fvi_solictud_traslado       ======> YA NO SE USA



                //10011     fvi_pedidos_bloqueados
                regHANA = 0;
                regPostgrest = 0;
                datetime_ini = DateTime.Now.ToString("hh:mm:ss tt");
                foreach (var sociedad in sociedades)
                {
                    regHANA = await SAP_pedidos_bloqueados(sociedad);
                }
                if (PedidoBloqALL.Count > 0)
                {
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10011, 1);//1=ELIMINA TABLA TMP / 2= PRD
                    reg_update = await PBI.clsMaestroProductos.carga_pedidos_bloqueados();
                    datetime_fin = DateTime.Now.ToString("hh:mm:ss tt");
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10011, 2);//1=ELIMINA TABLA TMP / 2= PRD
                    reg_update = await PBI.clsMaestroProductos.carga_data_update("fvi_pedidos_bloqueados", PedidoBloqALL.Count, DateTime.Now, datetime_ini, datetime_fin);
                    Console.WriteLine("11.- PEDIDOS BLOQUEADOS : " + datetime_ini + " - " + datetime_fin + " - " + PedidoBloqALL.Count);
                    PBI.clsMaestroProductos.registrar_datos("11.- PEDIDOS BLOQUEADOS : " + datetime_ini + " - " + datetime_fin + " - " + PedidoBloqALL.Count);
                }
                else
                {
                    Console.WriteLine(".... ERROR 11 PEDIDOS BLOQUEADOS SIN DATOS SAP : " + datetime_ini + " - " + datetime_ini);
                    PBI.clsMaestroProductos.registrar_error("TODAS", 1, "SIN FECHAS - 11 PEDIDOS BLOQUEADOS", "SIN DATOS DESDE SAP");
                }



                //10012     fvi_oferta_ventas
                regHANA = 0;
                datetime_ini = DateTime.Now.ToString("hh:mm:ss tt");
                foreach (var sociedad in sociedades)
                {
                    regHANA = await SAP_oferta_ventas(sociedad);
                }
                if (OfertaVtaALL.Count > 0)
                {
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10012, 1);//1=ELIMINA TABLA TMP / 2= PRD
                    reg_update = await PBI.clsMaestroProductos.carga_oferta_ventas();
                    datetime_fin = DateTime.Now.ToString("hh:mm:ss tt");
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10012, 2);//1=ELIMINA TABLA TMP / 2= PRD
                    reg_update = await PBI.clsMaestroProductos.carga_data_update("fvi_oferta_ventas", OfertaVtaALL.Count, DateTime.Now, datetime_ini, datetime_fin);
                    Console.WriteLine("12.- OFERTA DE VENTAS: " + datetime_ini + " - " + datetime_fin + " - " + OfertaVtaALL.Count);
                    PBI.clsMaestroProductos.registrar_datos("12.- OFERTA DE VENTAS: " + datetime_ini + " - " + datetime_fin + " - " + OfertaVtaALL.Count);
                }
                else
                {
                    Console.WriteLine(".... ERROR 12 OFERTA D EVENTAS SIN DATOS SAP : " + datetime_ini + " - " + datetime_ini);
                    PBI.clsMaestroProductos.registrar_error("TODAS", 1, "SIN FECHAS - 12 OFERTA DE VENTAS", "SIN DATOS DESDE SAP");
                }



                //10017     fvi_guias_facturas
                regHANA = 0;
                regPostgrest = 0;
                datetime_ini = DateTime.Now.ToString("hh:mm:ss tt");
                foreach (var sociedad in sociedades)
                {
                    regHANA = await SAP_Guias_Facturas(sociedad);
                }
                if (GuiaFacturaALL.Count > 0)
                {
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10017, 1);//1=ELIMINA TABLA TMP
                    reg_update = await PBI.clsMaestroProductos.cargaGuias_Facturas();//carga_PRODUCTOS
                    datetime_fin = DateTime.Now.ToString("hh:mm:ss tt");
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10017, 2);//1=ELIMINA TABLA TMP
                    reg_update = await PBI.clsMaestroProductos.carga_data_update("fvi_guias_facturas", GuiaFacturaALL.Count, DateTime.Now, datetime_ini, datetime_fin);
                    Console.WriteLine("17.- GUIAS V/S FACTURAS : " + datetime_ini + " - " + datetime_fin + " - " + GuiaFacturaALL.Count);
                    PBI.clsMaestroProductos.registrar_datos("17.- GUIAS V/S FACTURAS : " + datetime_ini + " - " + datetime_fin + " - " + GuiaFacturaALL.Count);
                }
                else
                {
                    Console.WriteLine(".... ERROR 17 GUIAS V/S FACTURAS SIN DATOS SAP : " + datetime_ini + " - " + datetime_ini);
                    PBI.clsMaestroProductos.registrar_error("TODAS", 1, "SIN FECHAS - 17 GUIAS V/S FACTURAS", "SIN DATOS DESDE SAP");
                }


                //10018     fvi_tarjetas
                regHANA = 0;
                datetime_ini = DateTime.Now.ToString("hh:mm:ss tt");
                foreach (var sociedad in sociedades)
                {
                    regHANA = await SAP_tarjetas(sociedad);
                }
                if (TarjetasALL.Count > 0)
                {
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10018, 1);//1=ELIMINA TABLA TMP
                    reg_update = await PBI.clsMaestroProductos.carga_tarjetas();
                    datetime_fin = DateTime.Now.ToString("hh:mm:ss tt");
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10018, 2);//2=CARGA TABLA PRD
                    reg_update = await PBI.clsMaestroProductos.carga_data_update("fvi_tarjetas", TarjetasALL.Count, DateTime.Now, datetime_ini, datetime_fin);
                    Console.WriteLine("18.- TARJETAS : " + datetime_ini + " - " + datetime_fin + " - " + TarjetasALL.Count);
                    PBI.clsMaestroProductos.registrar_datos("18.- TARJETAS : " + datetime_ini + " - " + datetime_fin + " - " + TarjetasALL.Count);
                }
                else
                {
                    Console.WriteLine(".... ERROR 18.- TARJETAS SIN DATOS SAP : " + datetime_ini + " - " + datetime_ini);
                    PBI.clsMaestroProductos.registrar_error("TODAS", 1, "18.- TARJETAS", "SIN DATOS DESDE SAP");
                }

                //10020     fvi_articulo_stock_pallet
                regHANA = 0;
                datetime_ini = DateTime.Now.ToString("hh:mm:ss tt");
                foreach (var sociedad in sociedades)
                {
                    regHANA = await SAP_pallets(sociedad);
                }
                if (PalletsALL.Count > 0)
                {
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10020, 1);//1=ELIMINA TABLA TMP
                    reg_update = await PBI.clsMaestroProductos.carga_Pallets();
                    datetime_fin = DateTime.Now.ToString("hh:mm:ss tt");
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10020, 2);//2=CARGA TABLA PRD
                    reg_update = await PBI.clsMaestroProductos.carga_data_update("fvi_articulo_stock_pallet", PalletsALL.Count, DateTime.Now, datetime_ini, datetime_fin);
                    Console.WriteLine("20.- MAESTRO_PALLETS : " + datetime_ini + " - " + datetime_fin + " - " + PalletsALL.Count);
                    PBI.clsMaestroProductos.registrar_datos("20.- MAESTRO_PALLETS : " + datetime_ini + " - " + datetime_fin + " - " + PalletsALL.Count);
                }
                else
                {
                    Console.WriteLine(".... ERROR 20.- MAESTRO_PALLETS SIN DATOS SAP : " + datetime_ini + " - " + datetime_ini);
                    PBI.clsMaestroProductos.registrar_error("TODAS", 1, "20.- MAESTRO_PALLETS", "SIN DATOS DESDE SAP");
                }

                //10021     fvi_medidas_articulo_dor
                regHANA = 0;
                datetime_ini = DateTime.Now.ToString("hh:mm:ss tt");
                foreach (var sociedad in sociedades)
                {
                    regHANA = await SAP_articulos(sociedad);
                }
                if (ArticulosALL.Count > 0)
                {
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10021, 1);//1=ELIMINA TABLA TMP
                    reg_update = await PBI.clsMaestroProductos.carga_Articulos();
                    datetime_fin = DateTime.Now.ToString("hh:mm:ss tt");
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10021, 2);//2=CARGA TABLA PRD
                    reg_update = await PBI.clsMaestroProductos.carga_data_update("fvi_medidas_articulo_dor", ArticulosALL.Count, DateTime.Now, datetime_ini, datetime_fin);
                    Console.WriteLine("21.- MAESTRO_ARTICULOS : " + datetime_ini + " - " + datetime_fin + " - " + ArticulosALL.Count);
                    PBI.clsMaestroProductos.registrar_datos("21.- MAESTRO_ARTICULOS : " + datetime_ini + " - " + datetime_fin + " - " + ArticulosALL.Count);
                }
                else
                {
                    Console.WriteLine(".... ERROR 21.- MAESTRO_ARTICULOS SIN DATOS SAP : " + datetime_ini + " - " + datetime_ini);
                    PBI.clsMaestroProductos.registrar_error("TODAS", 1, "21.- MAESTRO_ARTICULOS", "SIN DATOS DESDE SAP");
                }


                //10022        fvi_nombrespropiedades
                regHANA = 0;
                datetime_ini = DateTime.Now.ToString("hh:mm:ss tt");
                foreach (var sociedad in sociedades)
                {
                    regHANA = await SAP_nombrespropiedades(sociedad);
                }
                if (NombresPropiedadesALL.Count > 0)
                {
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10022, 1);//1=ELIMINA TABLA TMP
                    reg_update = await PBI.clsMaestroProductos.carga_NombresPropiedades();
                    datetime_fin = DateTime.Now.ToString("hh:mm:ss tt");
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10022, 2);//2=CARGA TABLA PRD
                    reg_update = await PBI.clsMaestroProductos.carga_data_update("fvi_nombrespropiedades", NombresPropiedadesALL.Count, DateTime.Now, datetime_ini, datetime_fin);
                    Console.WriteLine("22.- MAESTRO_NOMBRESPROPIEDADES_DOR : " + datetime_ini + " - " + datetime_fin + " - " + NombresPropiedadesALL.Count);
                    PBI.clsMaestroProductos.registrar_datos("22.- MAESTRO_NOMBRESPROPIEDADES_DOR : " + datetime_ini + " - " + datetime_fin + " - " + NombresPropiedadesALL.Count);
                }
                else
                {
                    Console.WriteLine(".... ERROR 22.- MAESTRO_NOMBRESPROPIEDADES_DOR SIN DATOS SAP : " + datetime_ini + " - " + datetime_ini);
                    PBI.clsMaestroProductos.registrar_error("TODAS", 1, "22.- MAESTRO_NOMBRESPROPIEDADES_DOR", "SIN DATOS DESDE SAP");
                }

                //10023        fvi_entrada_mercaderia_dor
                regHANA = 0;
                datetime_ini = DateTime.Now.ToString("hh:mm:ss tt");
                foreach (var sociedad in sociedades)
                {
                    regHANA = await SAP_entradamercaderia(sociedad);
                }
                if (EntradaMercaderiaALL.Count > 0)
                {
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10023, 1);//1=ELIMINA TABLA TMP
                    reg_update = await PBI.clsMaestroProductos.carga_EntradaMercaderia();
                    datetime_fin = DateTime.Now.ToString("hh:mm:ss tt");
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10023, 2);//2=CARGA TABLA PRD
                    reg_update = await PBI.clsMaestroProductos.carga_data_update("fvi_entrada_mercaderia_dor", EntradaMercaderiaALL.Count, DateTime.Now, datetime_ini, datetime_fin);
                    Console.WriteLine("23.- MAESTRO_ENTRADAMERCADERIA_DOR : " + datetime_ini + " - " + datetime_fin + " - " + EntradaMercaderiaALL.Count);
                    PBI.clsMaestroProductos.registrar_datos("23.- MAESTRO_ENTRADAMERCADERIA_DOR : " + datetime_ini + " - " + datetime_fin + " - " + EntradaMercaderiaALL.Count);
                }
                else
                {
                    Console.WriteLine(".... ERROR 23.- MAESTRO_ENTRADAMERCADERIA_DOR SIN DATOS SAP : " + datetime_ini + " - " + datetime_ini);
                    PBI.clsMaestroProductos.registrar_error("TODAS", 1, "23.- MAESTRO_ENTRADAMERCADERIA_DOR", "SIN DATOS DESDE SAP");
                }


                //10024        fvi_propiedades_productos_dor
                regHANA = 0;
                datetime_ini = DateTime.Now.ToString("hh:mm:ss tt");
                foreach (var sociedad in sociedades)
                {
                    regHANA = await SAP_propiedadesproductos(sociedad);
                }
                if (PropiedadesProductosALL.Count > 0)
                {
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10024, 1);//1=ELIMINA TABLA TMP
                    reg_update = await PBI.clsMaestroProductos.carga_PropiedadesProductos();
                    datetime_fin = DateTime.Now.ToString("hh:mm:ss tt");
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10024, 2);//2=CARGA TABLA PRD
                    reg_update = await PBI.clsMaestroProductos.carga_data_update("fvi_propiedades_productos_dor", PropiedadesProductosALL.Count, DateTime.Now, datetime_ini, datetime_fin);
                    Console.WriteLine("24.- MAESTRO_PROPIEDADES_PRODUCTOS_DOR : " + datetime_ini + " - " + datetime_fin + " - " + PropiedadesProductosALL.Count);
                    PBI.clsMaestroProductos.registrar_datos("24.- MAESTRO_PROPIEDADES_PRODUCTOS_DOR : " + datetime_ini + " - " + datetime_fin + " - " + PropiedadesProductosALL.Count);
                }
                else
                {
                    Console.WriteLine(".... ERROR 24.- MAESTRO_PROPIEDADES_PRODUCTOS_DOR SIN DATOS SAP : " + datetime_ini + " - " + datetime_ini);
                    PBI.clsMaestroProductos.registrar_error("TODAS", 1, "24.- MAESTRO_PROPIEDADES_PRODUCTOS_DOR", "SIN DATOS DESDE SAP");
                }


                datetime_ini = DateTime.Now.ToString("hh:mm:ss tt");
                return "FIN SIN FECHA =====> " + datetime_ini;
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error("TODAS", 1, "cargas_sin_fechas", e.Message);
                return "ERROR -->" + err;
            }

        }

        public static async Task<string> cargas_con_fechas()
        {
            string err = "";
            string rta = "";
            int regHANA = 0;
            int regPostgrest = 0;
            string reg_update = "";

            string fi_t = "";
            string ff_t = "";
            int flag = 0;
            int tope = 0;
            int xdias = 0;
            int cantReg = 0;
            try
            {
                //==============>   CON PARAMETROS DE FECHAS ============================================
                string datetime_ini = DateTime.Now.ToString("hh:mm:ss tt");
                string datetime_fin = DateTime.Now.ToString("hh:mm:ss tt");
                string my_retorno = "";
                
                 //10013     fvi_libro_mayor
                regHANA = 0;
                datetime_ini = DateTime.Now.ToString("hh:mm:ss tt");

                regHANA = await SAP_libro_mayor("Chile", FD8, FH8);//CHILE
                //regHANA = await SAP_libro_mayor("Perú", FD8, FH8);//PERU
                if (LibroMayorALL.Count > 0)
                {
                    //ElimanA datos de la tabla
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10013, 1);//1=ELIMINA TABLA TMP / 2= PRD
                    my_retorno = await PBI.clsMaestroProductos.carga_libro_mayorV2();
                    datetime_fin = DateTime.Now.ToString("hh:mm:ss tt");
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10013, 2);//1=ELIMINA TABLA TMP / 2= PRD
                    reg_update = await PBI.clsMaestroProductos.carga_data_update("fvi_libro_mayor", LibroMayorALL.Count, DateTime.Now, datetime_ini, datetime_fin);
                    Console.WriteLine("13.- LIBRO MAYOR : " + datetime_ini + " - " + datetime_fin + " - " + LibroMayorALL.Count);
                    PBI.clsMaestroProductos.registrar_datos("13.- LIBRO MAYOR : " + datetime_ini + " - " + datetime_fin + " - " + LibroMayorALL.Count);
                }
                else
                {
                    Console.WriteLine(".... ERROR 13 LIBRO MAYOR SIN DATOS SAP : " + datetime_ini + " - " + datetime_ini);
                    PBI.clsMaestroProductos.registrar_error("TODAS", 1, "13 LIBRO MAYOR", "SIN DATOS DESDE SAP");
                }


                //10014     fvi_ventas
                fi_t = "";
                ff_t = "";
                flag = 0;
                tope = 0;
                xdias = 0;
                cantReg = 0;

                tope = Math.Abs((FFF_F.Month - FII_F.Month) + 12 * (FFF_F.Year - FII_F.Year));
                xdias = (Convert.ToInt32(FII_F.Day) - 1) * -1;
                datetime_ini = DateTime.Now.ToString("hh:mm:ss tt");

                //Elimanr datos de la tabla
                PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10014, 1);//1=ELIMINA TABLA TMP / 2= PRD
                foreach (var sociedad in sociedades)
                {
                    flag = 0;
                    fi_t = "";
                    ff_t = "";
                    FII_F = FII_FResp;
                    FFF_F = FFF_FResp;
                    FII_F = FII_F.AddDays(xdias);
                    FFF_F = FII_F.AddMonths(1).AddDays(-1);

                    while (flag <= tope)
                    {
                        regHANA = 0;
                        regPostgrest = 0;
                        fi_t = Convert.ToString(FII_F).Substring(6, 4) + Convert.ToString(FII_F).Substring(3, 2) + Convert.ToString(FII_F).Substring(0, 2);
                        ff_t = Convert.ToString(FFF_F).Substring(6, 4) + Convert.ToString(FFF_F).Substring(3, 2) + Convert.ToString(FFF_F).Substring(0, 2);

                        DetalleVtasDOALL.Clear();
                        regHANA = await SAP_detalleVentaArticulos_newDO_V2(sociedad, fi_t, ff_t);

                        FII_F = FII_F.AddMonths(1);
                        FFF_F = FII_F.AddMonths(1).AddDays(-1);
                        flag = flag + 1;
                        reg_update = await PBI.clsMaestroProductos.carga_detalleVtaArticulos_newDO();
                        cantReg = cantReg + DetalleVtasDOALL.Count;
                        Console.WriteLine(" - " + sociedad + " ===> " + fi_t + " / " + ff_t + " ===> Cant:  " + DetalleVtasDOALL.Count + " * Acumulado:  " + cantReg);
                        PBI.clsMaestroProductos.registrar_datos(" - " + sociedad + " ===> " + fi_t + " / " + ff_t + " ===> Cant:  " + DetalleVtasDOALL.Count + " * Acumulado:  " + cantReg);
                    }
                }
                Console.WriteLine("");
                PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10014, 2);//1=ELIMINA TABLA TMP / 2= PRD
                datetime_fin = DateTime.Now.ToString("hh:mm:ss tt");
                reg_update = await PBI.clsMaestroProductos.carga_data_update("fvi_ventas", cantReg, DateTime.Now, datetime_ini, datetime_fin);//DetalleVtasALL.Count
                Console.WriteLine("14.- VENTAS  : " + datetime_ini + " - " + datetime_fin + " - " + cantReg);//DetalleVtasALL.Count
                PBI.clsMaestroProductos.registrar_datos("14.- VENTAS  : " + datetime_ini + " - " + datetime_fin + " - " + cantReg);

                //10015     fvi_reporte_ot
                regHANA = 0;
                regPostgrest = 0;
                datetime_ini = DateTime.Now.ToString("hh:mm:ss tt");
                foreach (var sociedad in sociedades)
                {
                    if (sociedad != "PERU")
                        regHANA = await SAP_sstt_reortOT(sociedad, FD6, FH6);
                }
                if (SSTTrepALL.Count > 0)
                {
                    //ElimanA datos de la tabla
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10015, 1);//1=ELIMINA TABLA TMP / 2= PRD
                    my_retorno = await PBI.clsMaestroProductos.carga_sstt_reportes();
                    datetime_fin = DateTime.Now.ToString("hh:mm:ss tt");
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10015, 2);//1=ELIMINA TABLA TMP / 2= PRD
                    reg_update = await PBI.clsMaestroProductos.carga_data_update("fvi_reporte_ot", SSTTrepALL.Count, DateTime.Now, datetime_ini, datetime_fin);
                    Console.WriteLine("15.- REPORTE DE OT: " + datetime_ini + " - " + datetime_fin + " - " + SSTTrepALL.Count);
                    PBI.clsMaestroProductos.registrar_datos("15.- REPORTE DE OT: " + datetime_ini + " - " + datetime_fin + " - " + SSTTrepALL.Count);
                }
                else
                {
                    Console.WriteLine(".... ERROR 15 REPORTE DE OT CON DATOS SAP : " + datetime_ini + " - " + datetime_ini);
                    PBI.clsMaestroProductos.registrar_error("TODAS", 1, "CON FECHAS - 15 REPORTE DE OT ", "SIN DATOS DESDE SAP");
                }

                
                //10016     fvi_nota_vtas_digitadas
                regHANA = 0;
                regPostgrest = 0;
                datetime_ini = DateTime.Now.ToString("hh:mm:ss tt");
                foreach (var sociedad in sociedades)
                {
                    regHANA = await SAP_notaVentas_digitadas(sociedad, FD7, FH7);
                }
                if (NVtaDigALL.Count > 0)
                {
                    //ElimanA datos de la tabla
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10016, 1);//1=ELIMINA TABLA TMP / 2= PRD
                    my_retorno = await PBI.clsMaestroProductos.carga_nventas_digitadas();
                    datetime_fin = DateTime.Now.ToString("hh:mm:ss tt");
                    PBI.clsMaestroProductos.CONTROL_CARGAS_ESPEJO(10016, 2);//1=ELIMINA TABLA TMP / 2= PRD
                    reg_update = await PBI.clsMaestroProductos.carga_data_update("fvi_nota_vtas_digitadas", NVtaDigALL.Count, DateTime.Now, datetime_ini, datetime_fin);
                    Console.WriteLine("16.- NOTAS DE VENTAS: " + datetime_ini + " - " + datetime_fin + " - " + NVtaDigALL.Count);
                    PBI.clsMaestroProductos.registrar_datos("16.- NOTAS DE VENTAS: " + datetime_ini + " - " + datetime_fin + " - " + NVtaDigALL.Count);
                }
                else
                {
                    Console.WriteLine(".... ERROR 16.-  NOTAS DE VENTASSIN DATOS SAP : " + datetime_ini + " - " + datetime_ini);
                    PBI.clsMaestroProductos.registrar_error("TODAS", 1, "SIN FECHAS - 16- NOTAS DE VENTAS", "SIN DATOS DESDE SAP");
                }

                return "FIN CONSULTAS CON FECHAS..... "; 
            } 
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error("TODAS", 1, "cargas_con_fechas", e.Message);
                return "ERROR -->" + err;
            }

               
        }

        private static async Task<int> SAP_notaVentas_digitadas(string tsEMpresa, string fdesde, string fhasta)
        {
            string err = "";
            try
            {
                int tiSkip = 0;
                int tiflag = 1;
                int cantSLayer = 0;
                int cantRegHANA = 0;
                List<NVtaDig> NVtaDig = new List<NVtaDig>();
                NVtaDig = await get_ETL_nventasDigitadas(session, tiSkip, tsEMpresa, fdesde, fhasta);
                NVtaDigALL.AddRange(NVtaDig);
                cantSLayer = NVtaDig.Count;
                cantRegHANA = NVtaDigALL.Count;
                return cantRegHANA;
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "SAP_notaVentas_digitadas", e.Message);
                Console.WriteLine("ERROR -->" + err + " " + tiSkip);
                return -1;
            }
        }
        public static async Task<List<NVtaDig>> get_ETL_nventasDigitadas(string session, int tiSkip, string tsEMpresa, string fdesde, string fhasta)
        {
            string err = "";

            try
            {
                HttpClientHandler clientHandler = new HttpClientHandler();
                var cookieContainer = new CookieContainer();
                clientHandler.CookieContainer = cookieContainer;
                clientHandler.ServerCertificateCustomValidationCallback = (sender, cert, chain, sslPolicyErrors) => { return true; };
                using (var _client = new HttpClient(clientHandler))
                {
                    _client.BaseAddress = new Uri("https://119.8.155.113:50000/b1s/v1/sml.svc/");
                    cookieContainer.Add(_client.BaseAddress, new Cookie("B1SESSION", session));
                    _client.DefaultRequestHeaders.Accept.Clear();
                    _client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                    _client.DefaultRequestHeaders.Add("Prefer", "odata.maxpagesize=0");

                    var httpResponse = await _client.GetAsync("FU_NOTA_VTAS_DIGITADASParameters(FromDate='" + fdesde + "',ToDate='" + fhasta + "')/FU_NOTA_VTAS_DIGITADAS?$filter=Empresa eq '" + tsEMpresa + "'");
                    var responseContent = await httpResponse.Content.ReadAsStringAsync();
                    var respuesta = JsonConvert.DeserializeObject<ODataResponse<NVtaDig>>(responseContent);
                    return respuesta.Value;
                }
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "get_ETL_nventasDigitadas", e.Message);
                return null;
            }
        }

        private static async Task<int> SAP_sstt_reortOT(string tsEMpresa, string fdesde, string fhasta)
        {
            string err = "";
            try
            {
                int tiSkip = 0;
                int tiflag = 1;
                int cantSLayer = 0;
                int cantRegHANA = 0;
                List<SSTTrep> SSTTrep = new List<SSTTrep>();
                SSTTrep = await get_ETL_sstt_reportot(session, tiSkip, tsEMpresa, fdesde, fhasta);
                SSTTrepALL.AddRange(SSTTrep);
                cantSLayer = SSTTrep.Count;
                cantRegHANA = SSTTrep.Count;
                return cantRegHANA;
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "SAP_sstt_reortOT", e.Message);
                Console.WriteLine("ERROR -->" + err + " " + tiSkip);
                return -1;
            }

        }
        public static async Task<List<SSTTrep>> get_ETL_sstt_reportot(string session, int tiSkip, string tsEMpresa, string fdesde, string fhasta)
        {
            string err = "";

            try
            {
                HttpClientHandler clientHandler = new HttpClientHandler();
                var cookieContainer = new CookieContainer();
                clientHandler.CookieContainer = cookieContainer;
                clientHandler.ServerCertificateCustomValidationCallback = (sender, cert, chain, sslPolicyErrors) => { return true; };
                using (var _client = new HttpClient(clientHandler))
                {
                    _client.BaseAddress = new Uri("https://119.8.155.113:50000/b1s/v1/sml.svc/");
                    cookieContainer.Add(_client.BaseAddress, new Cookie("B1SESSION", session));
                    _client.DefaultRequestHeaders.Accept.Clear();
                    _client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                    _client.DefaultRequestHeaders.Add("Prefer", "odata.maxpagesize=0");

                    var httpResponse = await _client.GetAsync("FU_REPORTE_OTParameters(FromDate='" + fdesde + "',ToDate='" + fhasta + "')/FU_REPORTEt_OT?$filter=Empresa eq '" + tsEMpresa + "'");
                    var responseContent = await httpResponse.Content.ReadAsStringAsync();
                    var respuesta = JsonConvert.DeserializeObject<ODataResponse<SSTTrep>>(responseContent);

                    return respuesta.Value;
                }
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "get_ETL_sstt_reportot", e.Message);
                return null;
            }
        }


        private static async Task<int> SAP_detalleVentaArticulos_newDO_V2(string tsEMpresa, string fdesde, string fhasta)
        {
            string err = "";
            try
            {
                int tiSkip = 0;
                int cantSLayer = 0;
                int cantRegHANA = 0;
                List<DetalleVtasDO> DetalleVtas = new List<DetalleVtasDO>();
                DetalleVtas = await get_ETL_detalle_vta_articulos_newDO_v2(session, tiSkip, tsEMpresa, fdesde, fhasta);
                DetalleVtasDOALL.AddRange(DetalleVtas);
                cantSLayer = DetalleVtas.Count;
                cantRegHANA = DetalleVtasDOALL.Count;
                return cantRegHANA;
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "SAP_detalleVentaArticulos", e.Message);
                Console.WriteLine("ERROR -->" + err + " " + tiSkip);
                return -1;
            }

        }
        public static async Task<List<DetalleVtasDO>> get_ETL_detalle_vta_articulos_newDO_v2(string session, int tiSkip, string tsEMpresa, string fdesde, string fhasta)
        {
            string err = "";

            try
            {
                HttpClientHandler clientHandler = new HttpClientHandler();
                var cookieContainer = new CookieContainer();
                clientHandler.CookieContainer = cookieContainer;
                clientHandler.ServerCertificateCustomValidationCallback = (sender, cert, chain, sslPolicyErrors) => { return true; };
                using (var _client = new HttpClient(clientHandler))
                {
                    _client.BaseAddress = new Uri("https://119.8.155.113:50000/b1s/v1/sml.svc/");
                    cookieContainer.Add(_client.BaseAddress, new Cookie("B1SESSION", session));
                    _client.DefaultRequestHeaders.Accept.Clear();
                    _client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                    _client.DefaultRequestHeaders.Add("Prefer", "odata.maxpagesize=0");
                    var httpResponse = await _client.GetAsync("FU_VENTASParameters(FromDate='" + fdesde + "',ToDate='" + fhasta + "')/FU_VENTAS?$filter=Empresa eq '" + tsEMpresa + "'");
                    var responseContent = await httpResponse.Content.ReadAsStringAsync();
                    var respuesta = JsonConvert.DeserializeObject<ODataResponse<DetalleVtasDO>>(responseContent);

                    return respuesta.Value;
                }
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "get_ETL_detalle_vta_articulos", e.Message);
                return null;
            }
        }

        private static async Task<int> SAP_libro_mayor(string tsEMpresa, string fdesde, string fhasta)
        {
            string err = "";
            try
            {
                int tiSkip = 0;
                int cantSLayer = 0;
                int cantRegHANA = 0;
                List<LibroMayor> LibroMayor = new List<LibroMayor>();
                LibroMayor = await get_ETL_libro_mayor(session, tiSkip, tsEMpresa, fdesde, fhasta);
                LibroMayorALL.AddRange(LibroMayor);
                cantSLayer = LibroMayor.Count;
                cantRegHANA = LibroMayorALL.Count;
                return cantRegHANA;
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "SAP_libro_mayor", e.Message);
                Console.WriteLine("ERROR -->" + err + " " + tiSkip);
                return -1;
            }
        }
        public static async Task<List<LibroMayor>> get_ETL_libro_mayor(string session, int tiSkip, string tsEMpresa, string fdesde, string fhasta)
        {
            string err = "";
            try
            {
                HttpClientHandler clientHandler = new HttpClientHandler();
                var cookieContainer = new CookieContainer();
                clientHandler.CookieContainer = cookieContainer;
                clientHandler.ServerCertificateCustomValidationCallback = (sender, cert, chain, sslPolicyErrors) => { return true; };
                using (var _client = new HttpClient(clientHandler))
                {
                    _client.BaseAddress = new Uri("https://119.8.155.113:50000/b1s/v1/sml.svc/");
                    cookieContainer.Add(_client.BaseAddress, new Cookie("B1SESSION", session));
                    _client.DefaultRequestHeaders.Accept.Clear();
                    _client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                    _client.DefaultRequestHeaders.Add("Prefer", "odata.maxpagesize=0");

                    var httpResponse = await _client.GetAsync("DS_LIBROMAYORParameters(FromDate='" + fdesde + "',ToDate='" + fhasta + "', Country= '" + tsEMpresa + "')/DS_LIBROMAYOR");
                    var responseContent = await httpResponse.Content.ReadAsStringAsync();
                    var respuesta = JsonConvert.DeserializeObject<ODataResponse<LibroMayor>>(responseContent);

                    return respuesta.Value;
                }
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "get_ETL_nventasDigitadas", e.Message);
                return null;
            }
        }




        private static async Task<int> SAP_oferta_ventas(string tsEMpresa)
        {
            string err = "";
            try
            {
                int tiSkip = 0;
                int tiflag = 1;
                int cantSLayer = 0;
                int cantRegHANA = 0;
                List<OfertaVta> OfertaVta = new List<OfertaVta>();
                OfertaVta = await get_ETL_oferta_ventas(session, tiSkip, tsEMpresa);
                OfertaVtaALL.AddRange(OfertaVta);
                cantSLayer = OfertaVta.Count;
                cantRegHANA = OfertaVta.Count;
                return cantRegHANA;
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "SAP_oferta_ventas", e.Message);
                Console.WriteLine("ERROR -->" + err + " " + tiSkip);
                return -1;
            }

        }
        public static async Task<List<OfertaVta>> get_ETL_oferta_ventas(string session, int tiSkip, string tsEMpresa)
        {
            string err = "";

            try
            {
                HttpClientHandler clientHandler = new HttpClientHandler();
                var cookieContainer = new CookieContainer();
                clientHandler.CookieContainer = cookieContainer;
                clientHandler.ServerCertificateCustomValidationCallback = (sender, cert, chain, sslPolicyErrors) => { return true; };
                using (var _client = new HttpClient(clientHandler))
                {
                    _client.BaseAddress = new Uri("https://119.8.155.113:50000/b1s/v1/sml.svc/");
                    cookieContainer.Add(_client.BaseAddress, new Cookie("B1SESSION", session));
                    _client.DefaultRequestHeaders.Accept.Clear();
                    _client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                    _client.DefaultRequestHeaders.Add("Prefer", "odata.maxpagesize=0");

                    var httpResponse = await _client.GetAsync("FU_OFERTA_VENTAS?$filter=Empresa eq '" + tsEMpresa + "'");
                    var responseContent = await httpResponse.Content.ReadAsStringAsync();
                    var respuesta = JsonConvert.DeserializeObject<ODataResponse<OfertaVta>>(responseContent);

                    return respuesta.Value;
                }
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "get_ETL_oferta_ventas", e.Message);
                return null;
            }
        }


        private static async Task<int> SAP_factura_reserva(string tsEMpresa)
        {
            string err = "";
            try
            {
                int tiSkip = 0;
                int cantSLayer = 0;
                int cantRegHANA = 0;
                List<FacturaReserv> FacturaReserv = new List<FacturaReserv>();
                FacturaReserv = await get_ETL_factura_reserva(session, tiSkip, tsEMpresa);
                FacturaReservALL.AddRange(FacturaReserv);
                cantSLayer = FacturaReserv.Count;
                cantRegHANA = FacturaReserv.Count;
                return cantRegHANA;
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "SAP_factura_reserva", e.Message);
                Console.WriteLine("ERROR -->" + err + " " + tiSkip);
                return -1;
            }

        }
        public static async Task<List<FacturaReserv>> get_ETL_factura_reserva(string session, int tiSkip, string tsEMpresa)
        {
            string err = "";

            try
            {
                HttpClientHandler clientHandler = new HttpClientHandler();
                var cookieContainer = new CookieContainer();
                clientHandler.CookieContainer = cookieContainer;
                clientHandler.ServerCertificateCustomValidationCallback = (sender, cert, chain, sslPolicyErrors) => { return true; };
                using (var _client = new HttpClient(clientHandler))
                {
                    _client.BaseAddress = new Uri("https://119.8.155.113:50000/b1s/v1/sml.svc/");
                    cookieContainer.Add(_client.BaseAddress, new Cookie("B1SESSION", session));
                    _client.DefaultRequestHeaders.Accept.Clear();
                    _client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                    _client.DefaultRequestHeaders.Add("Prefer", "odata.maxpagesize=0");

                    var httpResponse = await _client.GetAsync("FU_FACTURA_RESERVAS?$filter=Empresa eq '" + tsEMpresa + "'");
                    var responseContent = await httpResponse.Content.ReadAsStringAsync();
                    var respuesta = JsonConvert.DeserializeObject<ODataResponse<FacturaReserv>>(responseContent);

                    return respuesta.Value;
                }
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "get_ETL_factura_reserva", e.Message);
                return null;
            }
        }

        private static async Task<int> SAP_patidasAbiertas(string tsEMpresa)
        {

            string err = "";
            try
            {
                int tiSkip = 0;
                int cantSLayer = 0;
                int cantRegHANA = 0;
                List<PartidasAbiertas> PartidasAbiertas = new List<PartidasAbiertas>();
                PartidasAbiertas = await get_ETL_partidasAbiertas(session, tiSkip, tsEMpresa);
                PartidasAbiertasALL.AddRange(PartidasAbiertas);
                cantSLayer = PartidasAbiertas.Count;
                cantRegHANA = PartidasAbiertasALL.Count;
                return cantRegHANA;
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "SAP_patidasAbiertas", e.Message);
                Console.WriteLine("ERROR -->" + err + " " + tiSkip);
                return -1;
            }

        }
        public static async Task<List<PartidasAbiertas>> get_ETL_partidasAbiertas(string session, int tiSkip, string tsEMpresa)
        {
            string err = "";

            try
            {
                HttpClientHandler clientHandler = new HttpClientHandler();
                var cookieContainer = new CookieContainer();
                clientHandler.CookieContainer = cookieContainer;
                clientHandler.ServerCertificateCustomValidationCallback = (sender, cert, chain, sslPolicyErrors) => { return true; };
                using (var _client = new HttpClient(clientHandler))
                {
                    _client.BaseAddress = new Uri("https://119.8.155.113:50000/b1s/v1/sml.svc/");
                    cookieContainer.Add(_client.BaseAddress, new Cookie("B1SESSION", session));
                    _client.DefaultRequestHeaders.Accept.Clear();
                    _client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                    _client.DefaultRequestHeaders.Add("Prefer", "odata.maxpagesize=0");

                    var httpResponse = await _client.GetAsync("FU_PARTIDASABIERTAS?$filter=Empresa eq '" + tsEMpresa + "'");
                    var responseContent = await httpResponse.Content.ReadAsStringAsync();
                    var respuesta = JsonConvert.DeserializeObject<ODataResponse<PartidasAbiertas>>(responseContent);

                    return respuesta.Value;
                }
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "get_ETL_partidasAbiertas", e.Message);
                return null;
            }
        }

        private static async Task<int> SAP_pedidos_bloqueados(string tsEMpresa)
        {
            string err = "";
            try
            {
                int tiSkip = 0;
                int cantSLayer = 0;
                int cantRegHANA = 0;
                List<PedidoBloq> PedidoBloq = new List<PedidoBloq>();
                PedidoBloq = await get_ETL_pedido_bloqueados(session, tiSkip, tsEMpresa);
                PedidoBloqALL.AddRange(PedidoBloq);
                cantSLayer = PedidoBloq.Count;
                cantRegHANA = PedidoBloq.Count;
                return cantRegHANA;
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "SAP_pedidos_bloqueados", e.Message);
                Console.WriteLine("ERROR -->" + err + " " + tiSkip);
                return -1;
            }
        }
        public static async Task<List<PedidoBloq>> get_ETL_pedido_bloqueados(string session, int tiSkip, string tsEMpresa)
        {
            string err = "";

            try
            {
                HttpClientHandler clientHandler = new HttpClientHandler();
                var cookieContainer = new CookieContainer();
                clientHandler.CookieContainer = cookieContainer;
                clientHandler.ServerCertificateCustomValidationCallback = (sender, cert, chain, sslPolicyErrors) => { return true; };
                using (var _client = new HttpClient(clientHandler))
                {
                    _client.BaseAddress = new Uri("https://119.8.155.113:50000/b1s/v1/sml.svc/");
                    cookieContainer.Add(_client.BaseAddress, new Cookie("B1SESSION", session));
                    _client.DefaultRequestHeaders.Accept.Clear();
                    _client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                    _client.DefaultRequestHeaders.Add("Prefer", "odata.maxpagesize=0");

                    var httpResponse = await _client.GetAsync("FU_PEDIDOS_BLOQUEADOS?$filter=Empresa eq '" + tsEMpresa + "'");
                    var responseContent = await httpResponse.Content.ReadAsStringAsync();
                    var respuesta = JsonConvert.DeserializeObject<ODataResponse<PedidoBloq>>(responseContent);

                    return respuesta.Value;
                }
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "get_ETL_pedido_bloqueados", e.Message);
                return null;
            }
        }

        private static async Task<int> SAP_tarjetas(string tsEMpresa)
        {
            string err = "";
            try
            {
                int tiSkip = 0;
                int cantSLayer = 0;
                int cantRegHANA = 0;
                List<Tarjetas> Tarjetas = new List<Tarjetas>();
                Tarjetas = await get_ETL_TARJETAS(session, tiSkip, tsEMpresa);
                TarjetasALL.AddRange(Tarjetas);
                cantSLayer = Tarjetas.Count;
                cantRegHANA = TarjetasALL.Count;
                return cantRegHANA;
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "SAP_tarjetas", e.Message);
                Console.WriteLine("ERROR -->" + err + " " + tiSkip);
                return -1;
            }

        }

        public static async Task<List<Tarjetas>> get_ETL_TARJETAS(string session, int tiSkip, string tsEMpresa)
        {
            string err = "";

            try
            {
                HttpClientHandler clientHandler = new HttpClientHandler();
                var cookieContainer = new CookieContainer();
                clientHandler.CookieContainer = cookieContainer;
                clientHandler.ServerCertificateCustomValidationCallback = (sender, cert, chain, sslPolicyErrors) => { return true; };
                using (var _client = new HttpClient(clientHandler))
                {
                    _client.BaseAddress = new Uri("https://119.8.155.113:50000/b1s/v1/sml.svc/");
                    cookieContainer.Add(_client.BaseAddress, new Cookie("B1SESSION", session));
                    _client.DefaultRequestHeaders.Accept.Clear();
                    _client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                    _client.DefaultRequestHeaders.Add("Prefer", "odata.maxpagesize=0");

                    var httpResponse = await _client.GetAsync("FU_TARJETA_EQUIPOS?");
                    var responseContent = await httpResponse.Content.ReadAsStringAsync();
                    var respuesta = JsonConvert.DeserializeObject<ODataResponse<Tarjetas>>(responseContent);

                    return respuesta.Value;
                }
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "get_ETL_MAESTRO_VENDEDORES", e.Message);
                return null;
            }
        }

        private static async Task<int> SAP_pallets(string tsEMpresa)
        {
            string err = "";
            try
            {
                int tiSkip = 0;
                int cantSLayer = 0;
                int cantRegHANA = 0;
                List<Pallets> Pallets = new List<Pallets>();
                Pallets = await get_ETL_PALLETS(session, tiSkip, tsEMpresa);
                PalletsALL.AddRange(Pallets);
                cantSLayer = Pallets.Count;
                cantRegHANA = PalletsALL.Count;
                return cantRegHANA;
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "SAP_pallets", e.Message);
                Console.WriteLine("ERROR -->" + err + " " + tiSkip);
                return -1;
            }

        }

        public static async Task<List<Pallets>> get_ETL_PALLETS(string session, int tiSkip, string tsEMpresa)
        {
            string err = "";

            try
            {
                HttpClientHandler clientHandler = new HttpClientHandler();
                var cookieContainer = new CookieContainer();
                clientHandler.CookieContainer = cookieContainer;
                clientHandler.ServerCertificateCustomValidationCallback = (sender, cert, chain, sslPolicyErrors) => { return true; };
                using (var _client = new HttpClient(clientHandler))
                {
                    _client.BaseAddress = new Uri("https://119.8.155.113:50000/b1s/v1/sml.svc/");
                    cookieContainer.Add(_client.BaseAddress, new Cookie("B1SESSION", session));
                    _client.DefaultRequestHeaders.Accept.Clear();
                    _client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                    _client.DefaultRequestHeaders.Add("Prefer", "odata.maxpagesize=0");

                    var httpResponse = await _client.GetAsync("FU_ARTICULO_STOCK_PALLET?");
                    var responseContent = await httpResponse.Content.ReadAsStringAsync();
                    var respuesta = JsonConvert.DeserializeObject<ODataResponse<Pallets>>(responseContent);

                    return respuesta.Value;
                }
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "get_ETL_MAESTRO_VENDEDORES", e.Message);
                return null;
            }
        }

        private static async Task<int> SAP_nombrespropiedades(string tsEMpresa)
        {
            string err = "";
            try
            {
                int tiSkip = 0;
                int cantSLayer = 0;
                int cantRegHANA = 0;
                List<NombresPropiedades> nombresPropiedades = new List<NombresPropiedades>();
                nombresPropiedades = await get_ETL_NOMBRESPROPIEDADES(session, tiSkip, tsEMpresa);
                NombresPropiedadesALL.AddRange(nombresPropiedades);
                cantSLayer = nombresPropiedades.Count;
                cantRegHANA = NombresPropiedadesALL.Count;
                return cantRegHANA;
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "SAP_nombrespropiedades", e.Message);
                Console.WriteLine("ERROR -->" + err + " " + tiSkip);
                return -1;
            }

        }

        public static async Task<List<NombresPropiedades>> get_ETL_NOMBRESPROPIEDADES(string session, int tiSkip, string tsEMpresa)
        {
            string err = "";

            try
            {
                HttpClientHandler clientHandler = new HttpClientHandler();
                var cookieContainer = new CookieContainer();
                clientHandler.CookieContainer = cookieContainer;
                clientHandler.ServerCertificateCustomValidationCallback = (sender, cert, chain, sslPolicyErrors) => { return true; };
                using (var _client = new HttpClient(clientHandler))
                {
                    _client.BaseAddress = new Uri("https://119.8.155.113:50000/b1s/v1/sml.svc/");
                    cookieContainer.Add(_client.BaseAddress, new Cookie("B1SESSION", session));
                    _client.DefaultRequestHeaders.Accept.Clear();
                    _client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                    _client.DefaultRequestHeaders.Add("Prefer", "odata.maxpagesize=0");

                    var httpResponse = await _client.GetAsync("FU_NOMBRES_PROPIEDADES_DOR?");
                    var responseContent = await httpResponse.Content.ReadAsStringAsync();
                    var respuesta = JsonConvert.DeserializeObject<ODataResponse<NombresPropiedades>>(responseContent);

                    return respuesta.Value;
                }
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "get_ETL_NOMBRESPROPIEDADES", e.Message);
                return null;
            }
        }

        private static async Task<int> SAP_entradamercaderia(string tsEMpresa)
        {
            string err = "";
            try
            {
                int tiSkip = 0;
                int cantSLayer = 0;
                int cantRegHANA = 0;
                List<EntradaMercaderia> entradaMercaderiaDOR = new List<EntradaMercaderia>();
                entradaMercaderiaDOR = await get_ETL_ENTRADAMERCADERIA(session, tiSkip, tsEMpresa);
                EntradaMercaderiaALL.AddRange(entradaMercaderiaDOR);
                cantSLayer = entradaMercaderiaDOR.Count;
                cantRegHANA = EntradaMercaderiaALL.Count;
                return cantRegHANA;
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "SAP_entradamercaderia", e.Message);
                Console.WriteLine("ERROR -->" + err + " " + tiSkip);
                return -1;
            }

        }

        public static async Task<List<EntradaMercaderia>> get_ETL_ENTRADAMERCADERIA(string session, int tiSkip, string tsEMpresa)
        {
            string err = "";

            try
            {
                HttpClientHandler clientHandler = new HttpClientHandler();
                var cookieContainer = new CookieContainer();
                clientHandler.CookieContainer = cookieContainer;
                clientHandler.ServerCertificateCustomValidationCallback = (sender, cert, chain, sslPolicyErrors) => { return true; };
                using (var _client = new HttpClient(clientHandler))
                {
                    _client.BaseAddress = new Uri("https://119.8.155.113:50000/b1s/v1/sml.svc/");
                    cookieContainer.Add(_client.BaseAddress, new Cookie("B1SESSION", session));
                    _client.DefaultRequestHeaders.Accept.Clear();
                    _client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                    _client.DefaultRequestHeaders.Add("Prefer", "odata.maxpagesize=0");

                    
                    var httpResponse = await _client.GetAsync("FU_ENTRADA_MERCADERIA_DOR?");
                    var responseContent = await httpResponse.Content.ReadAsStringAsync();
                    var respuesta = JsonConvert.DeserializeObject<ODataResponse<EntradaMercaderia>>(responseContent);

                    return respuesta.Value;
                }
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "get_ETL_ENTRADAMERCADERIA", e.Message);
                return null;
            }
        }

        private static async Task<int> SAP_propiedadesproductos(string tsEMpresa)
        {
            string err = "";
            try
            {
                int tiSkip = 0;
                int cantSLayer = 0;
                int cantRegHANA = 0;
                List<PropiedadesProductos> propiedadesProductosDOR = new List<PropiedadesProductos>();
                propiedadesProductosDOR = await get_ETL_PROPIEDADESPRODUCTOS(session, tiSkip, tsEMpresa);
                PropiedadesProductosALL.AddRange(propiedadesProductosDOR);
                cantSLayer = propiedadesProductosDOR.Count;
                cantRegHANA = PropiedadesProductosALL.Count;
                return cantRegHANA;
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "SAP_propiedadesproductos", e.Message);
                Console.WriteLine("ERROR -->" + err + " " + tiSkip);
                return -1;
            }

        }

        public static async Task<List<PropiedadesProductos>> get_ETL_PROPIEDADESPRODUCTOS(string session, int tiSkip, string tsEMpresa)
        {
            string err = "";

            try
            {
                HttpClientHandler clientHandler = new HttpClientHandler();
                var cookieContainer = new CookieContainer();
                clientHandler.CookieContainer = cookieContainer;
                clientHandler.ServerCertificateCustomValidationCallback = (sender, cert, chain, sslPolicyErrors) => { return true; };
                using (var _client = new HttpClient(clientHandler))
                {
                    _client.BaseAddress = new Uri("https://119.8.155.113:50000/b1s/v1/sml.svc/");
                    cookieContainer.Add(_client.BaseAddress, new Cookie("B1SESSION", session));
                    _client.DefaultRequestHeaders.Accept.Clear();
                    _client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                    _client.DefaultRequestHeaders.Add("Prefer", "odata.maxpagesize=0");


                    var httpResponse = await _client.GetAsync("FU_PROPIEDADES_PRODUCTOS_DOR?");
                    var responseContent = await httpResponse.Content.ReadAsStringAsync();
                    var respuesta = JsonConvert.DeserializeObject<ODataResponse<PropiedadesProductos>>(responseContent);

                    return respuesta.Value;
                }
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "get_ETL_PROPIEDADESPRODUCTOS", e.Message);
                return null;
            }
        }

        private static async Task<int> SAP_articulos(string tsEMpresa)
        {
            string err = "";
            try
            {
                int tiSkip = 0;
                int cantSLayer = 0;
                int cantRegHANA = 0;
                List<Articulos> Articulos = new List<Articulos>();
                Articulos = await get_ETL_ARTICULOS(session, tiSkip, tsEMpresa);
                ArticulosALL.AddRange(Articulos);
                cantSLayer = Articulos.Count;
                cantRegHANA = ArticulosALL.Count;
                return cantRegHANA;
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "SAP_articulos", e.Message);
                Console.WriteLine("ERROR -->" + err + " " + tiSkip);
                return -1;
            }

        }

        public static async Task<List<Articulos>> get_ETL_ARTICULOS(string session, int tiSkip, string tsEMpresa)
        {
            string err = "";

            try
            {
                HttpClientHandler clientHandler = new HttpClientHandler();
                var cookieContainer = new CookieContainer();
                clientHandler.CookieContainer = cookieContainer;
                clientHandler.ServerCertificateCustomValidationCallback = (sender, cert, chain, sslPolicyErrors) => { return true; };
                using (var _client = new HttpClient(clientHandler))
                {
                    _client.BaseAddress = new Uri("https://119.8.155.113:50000/b1s/v1/sml.svc/");
                    cookieContainer.Add(_client.BaseAddress, new Cookie("B1SESSION", session));
                    _client.DefaultRequestHeaders.Accept.Clear();
                    _client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                    _client.DefaultRequestHeaders.Add("Prefer", "odata.maxpagesize=0");

                    var httpResponse = await _client.GetAsync("FU_MEDIDAS_ARTICULO_DOR?");
                    var responseContent = await httpResponse.Content.ReadAsStringAsync();
                    var respuesta = JsonConvert.DeserializeObject<ODataResponse<Articulos>>(responseContent);

                    return respuesta.Value;
                }
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "get_ETL_MAESTRO_VENDEDORES", e.Message);
                return null;
            }
        }


        private static async Task<int> SAP_vendedor(string tsEMpresa)
        {
            string err = "";
            try
            {
                int tiSkip = 0;
                int cantSLayer = 0;
                int cantRegHANA = 0;
                List<Vendedor> Vendedor = new List<Vendedor>();
                Vendedor = await get_ETL_MAESTRO_VENDEDORES(session, tiSkip, tsEMpresa);
                VendedorALL.AddRange(Vendedor);
                cantSLayer = Vendedor.Count;
                cantRegHANA = VendedorALL.Count;
                return cantRegHANA;
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "SAP_vendedor", e.Message);
                Console.WriteLine("ERROR -->" + err + " " + tiSkip);
                return -1;
            }

        }

        public static async Task<List<Vendedor>> get_ETL_MAESTRO_VENDEDORES(string session, int tiSkip, string tsEMpresa)
        {
            string err = "";

            try
            {
                HttpClientHandler clientHandler = new HttpClientHandler();
                var cookieContainer = new CookieContainer();
                clientHandler.CookieContainer = cookieContainer;
                clientHandler.ServerCertificateCustomValidationCallback = (sender, cert, chain, sslPolicyErrors) => { return true; };
                using (var _client = new HttpClient(clientHandler))
                {
                    _client.BaseAddress = new Uri("https://119.8.155.113:50000/b1s/v1/sml.svc/");
                    cookieContainer.Add(_client.BaseAddress, new Cookie("B1SESSION", session));
                    _client.DefaultRequestHeaders.Accept.Clear();
                    _client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                    _client.DefaultRequestHeaders.Add("Prefer", "odata.maxpagesize=0");

                    var httpResponse = await _client.GetAsync("FU_MAESTRO_VENDEDORES?$filter=Empresa eq '" + tsEMpresa +"'");
                    var responseContent = await httpResponse.Content.ReadAsStringAsync();
                    var respuesta = JsonConvert.DeserializeObject<ODataResponse<Vendedor>>(responseContent);

                    return respuesta.Value;
                }
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "get_ETL_MAESTRO_VENDEDORES", e.Message);
                return null;
            }
        } 

        private static async Task<int> SAP_socios_de_Negocios(string tsEMpresa)
        {
            string err = "";
            try
            {
                int tiSkip = 0;
                int tiflag = 1;
                int cantSLayer = 0;
                int cantRegHANA = 0;
                List<SocNegocio> SocNegocio = new List<SocNegocio>();
                SocNegocio = await get_ETL_socios_de_Negocios(session, tiSkip, tsEMpresa);
                SocNegocioALL.AddRange(SocNegocio);
                cantSLayer = SocNegocio.Count;
                cantRegHANA = SocNegocioALL.Count;
                return cantRegHANA;
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "SAP_socios_de_Negocios", e.Message);
                Console.WriteLine("ERROR -->" + err + " " + tiSkip);
                return -1;
            }

        }

        public static async Task<List<SocNegocio>> get_ETL_socios_de_Negocios(string session, int tiSkip, string tsEMpresa)
        {
            string err = "";

            try
            {
                HttpClientHandler clientHandler = new HttpClientHandler();
                var cookieContainer = new CookieContainer();
                clientHandler.CookieContainer = cookieContainer;
                clientHandler.ServerCertificateCustomValidationCallback = (sender, cert, chain, sslPolicyErrors) => { return true; };
                using (var _client = new HttpClient(clientHandler))
                {
                    _client.BaseAddress = new Uri("https://119.8.155.113:50000/b1s/v1/sml.svc/");
                    cookieContainer.Add(_client.BaseAddress, new Cookie("B1SESSION", session));
                    _client.DefaultRequestHeaders.Accept.Clear();
                    _client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                    _client.DefaultRequestHeaders.Add("Prefer", "odata.maxpagesize=0");

                    var httpResponse = await _client.GetAsync("FU_MAESTRO_SOC_NEGOCIO?$filter =Empresa eq '" + tsEMpresa + "'");
                    var responseContent = await httpResponse.Content.ReadAsStringAsync();
                    var respuesta = JsonConvert.DeserializeObject<ODataResponse<SocNegocio>>(responseContent);

                    return respuesta.Value;
                }
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "get_ETL_socios_de_Negocios", e.Message);
                return null;
            }
        }

        private static async Task<int> SAP_MAESTRO_BODEGAS(string tsEMpresa)
        {
            string err = "";
            try
            {
                int tiSkip = 0;
                int cantSLayer = 0;
                int cantRegHANA = 0;
                List<MaestoBodega> MaestoBodega = new List<MaestoBodega>();
                MaestoBodega = await get_ETL_MAESTRO_BODEGAS(session, tiSkip, tsEMpresa);
                MaestoBodegaALL.AddRange(MaestoBodega);
                cantSLayer = MaestoBodega.Count;
                cantRegHANA = MaestoBodegaALL.Count;
                return cantRegHANA;
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "SAP_OC_All_Estatus", e.Message);
                Console.WriteLine("ERROR -->" + err + " " + tiSkip);
                return -1;
            }

        }
        public static async Task<List<MaestoBodega>> get_ETL_MAESTRO_BODEGAS(string session, int tiSkip, string tsEMpresa)
        {
            string err = "";

            try
            {
                HttpClientHandler clientHandler = new HttpClientHandler();
                var cookieContainer = new CookieContainer();
                clientHandler.CookieContainer = cookieContainer;
                clientHandler.ServerCertificateCustomValidationCallback = (sender, cert, chain, sslPolicyErrors) => { return true; };
                using (var _client = new HttpClient(clientHandler))
                {
                    _client.BaseAddress = new Uri("https://119.8.155.113:50000/b1s/v1/sml.svc/");
                    cookieContainer.Add(_client.BaseAddress, new Cookie("B1SESSION", session));
                    _client.DefaultRequestHeaders.Accept.Clear();
                    _client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                    _client.DefaultRequestHeaders.Add("Prefer", "odata.maxpagesize=0");

                    var httpResponse = await _client.GetAsync("FU_MAESTRO_BODEGA?$filter =Empresa eq '" + tsEMpresa + "'");
                    var responseContent = await httpResponse.Content.ReadAsStringAsync();
                    var respuesta = JsonConvert.DeserializeObject<ODataResponse<MaestoBodega>>(responseContent);

                    return respuesta.Value;
                }
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "get_ETL_MAESTRO_BODEGAS", e.Message);
                return null;
            }
        }

        //XXX
        private static async Task<int> SAP_Guias_Facturas(string tsEMpresa)
        {
            string err = "";
            try
            {
                int tiSkip = 0;
                int cantSLayer = 0;
                int cantRegHANA = 0;
                List<GuiaFactura> GuiaFactura = new List<GuiaFactura>();
                GuiaFactura = await get_ETL_Guias_Facturas(session, tiSkip, tsEMpresa);
                GuiaFacturaALL.AddRange(GuiaFactura);
                cantSLayer = GuiaFactura.Count;
                cantRegHANA = GuiaFacturaALL.Count;
                return cantRegHANA;
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "SAP_productos", e.Message);
                Console.WriteLine("ERROR -->" + err + " " + tiSkip);
                return -1;
            }
        }
        public static async Task<List<GuiaFactura>> get_ETL_Guias_Facturas(string session, int tiSkip, string tsEMpresa)
        {
            string err = "";

            try
            {
                HttpClientHandler clientHandler = new HttpClientHandler();
                var cookieContainer = new CookieContainer();
                clientHandler.CookieContainer = cookieContainer;
                clientHandler.ServerCertificateCustomValidationCallback = (sender, cert, chain, sslPolicyErrors) => { return true; };
                using (var _client = new HttpClient(clientHandler))
                {
                    _client.BaseAddress = new Uri("https://119.8.155.113:50000/b1s/v1/sml.svc/"); //_sl="h ttps://119.8.155.113:50000/b1s/v1/sml.svc/"
                    cookieContainer.Add(_client.BaseAddress, new Cookie("B1SESSION", session));
                    _client.DefaultRequestHeaders.Accept.Clear();
                    _client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                    _client.DefaultRequestHeaders.Add("Prefer", "odata.maxpagesize=0");

                    var httpResponse = await _client.GetAsync("FU_GUIAS_FACTURAS?$filter =Empresa eq '" + tsEMpresa + "'");
                    var responseContent = await httpResponse.Content.ReadAsStringAsync();
                    var respuesta = JsonConvert.DeserializeObject<ODataResponse<GuiaFactura>>(responseContent);

                    return respuesta.Value;
                }
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "get_ETL_MAESTRO_PRODUCTOS", e.Message);
                return null;
            }
        }


        //XXXX
        private static async Task<int> SAP_productos(string tsEMpresa)
        {
            string err = "";
            try
            {
                int tiSkip = 0;
                int cantSLayer = 0;
                int cantRegHANA = 0;
                List<Productos> Productos = new List<Productos>();
                Productos = await get_ETL_MAESTRO_PRODUCTOS(session, tiSkip, tsEMpresa);
                ProductosALL.AddRange(Productos);
                cantSLayer = Productos.Count;
                cantRegHANA = ProductosALL.Count;
                return cantRegHANA;
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "SAP_productos", e.Message);
                Console.WriteLine("ERROR -->" + err + " " + tiSkip);
                return -1;
            }
        }
        public static async Task<List<Productos>> get_ETL_MAESTRO_PRODUCTOS(string session, int tiSkip, string tsEMpresa)
        {
            string err = "";

            try
            {
                HttpClientHandler clientHandler = new HttpClientHandler();
                var cookieContainer = new CookieContainer();
                clientHandler.CookieContainer = cookieContainer;
                clientHandler.ServerCertificateCustomValidationCallback = (sender, cert, chain, sslPolicyErrors) => { return true; };
                using (var _client = new HttpClient(clientHandler))
                {
                    _client.BaseAddress = new Uri("https://119.8.155.113:50000/b1s/v1/sml.svc/"); //_sl="h ttps://119.8.155.113:50000/b1s/v1/sml.svc/"
                    cookieContainer.Add(_client.BaseAddress, new Cookie("B1SESSION", session));
                    _client.DefaultRequestHeaders.Accept.Clear();
                    _client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                    _client.DefaultRequestHeaders.Add("Prefer", "odata.maxpagesize=0");

                    var httpResponse = await _client.GetAsync("FU_MAESTRO_PRODUCTOS?$filter =Empresa eq '" + tsEMpresa + "'");
                    var responseContent = await httpResponse.Content.ReadAsStringAsync();
                    var respuesta = JsonConvert.DeserializeObject<ODataResponse<Productos>>(responseContent);

                    return respuesta.Value;
                }
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "get_ETL_MAESTRO_PRODUCTOS", e.Message);
                return null;
            }
        }

        private static async Task<int> SAP_lista_precios(string tsEMpresa)
        {
            string err = "";
            try
            {
                int tiSkip = 0;
                int cantSLayer = 0;
                int cantRegHANA = 0;
                List<ListaPrecio> ListaPrecio = new List<ListaPrecio>();
                ListaPrecio = await get_ETL_lista_de_precios(session, tiSkip, tsEMpresa);
                ListaPrecioALL.AddRange(ListaPrecio);
                cantSLayer = ListaPrecio.Count;
                cantRegHANA = ListaPrecioALL.Count;
                return cantRegHANA;
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "SAP_lista_precios", e.Message);
                Console.WriteLine("ERROR -->" + err + " " + tiSkip);
                return -1;
            }

        }
        public static async Task<List<ListaPrecio>> get_ETL_lista_de_precios(string session, int tiSkip, string tsEMpresa)
        {
            string err = "";

            try
            {
                HttpClientHandler clientHandler = new HttpClientHandler();
                var cookieContainer = new CookieContainer();
                clientHandler.CookieContainer = cookieContainer;
                clientHandler.ServerCertificateCustomValidationCallback = (sender, cert, chain, sslPolicyErrors) => { return true; };
                using (var _client = new HttpClient(clientHandler))
                {
                    _client.BaseAddress = new Uri("https://119.8.155.113:50000/b1s/v1/sml.svc/");
                    cookieContainer.Add(_client.BaseAddress, new Cookie("B1SESSION", session));
                    _client.DefaultRequestHeaders.Accept.Clear();
                    _client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                    _client.DefaultRequestHeaders.Add("Prefer", "odata.maxpagesize=0");

                    var httpResponse = await _client.GetAsync("FU_LISTA_PRECIOS?$filter=Empresa eq '" + tsEMpresa + "'");
                    var responseContent = await httpResponse.Content.ReadAsStringAsync();
                    var respuesta = JsonConvert.DeserializeObject<ODataResponse<ListaPrecio>>(responseContent);

                    return respuesta.Value;
                }
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "get_ETL_lista_de_precios", e.Message);
                return null;
            }
        }


        private static async Task<int> SAP_stock_de_productos(string tsEMpresa)
        {
            string err = "";
            try
            {
                int tiSkip = 0;
                int cantSLayer = 0;
                int cantRegHANA = 0;
                List<StockProducto> StockProducto = new List<StockProducto>();
                StockProducto = await get_ETL_stock_de_productos(session, tiSkip, tsEMpresa);
                StockProductoALL.AddRange(StockProducto);
                cantSLayer = StockProducto.Count;
                cantRegHANA = StockProductoALL.Count;
                return cantRegHANA;
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "SAP_stock_de_productos", e.Message);
                Console.WriteLine("ERROR -->" + err + " " + tiSkip);
                return -1;
            }

        }
        public static async Task<List<StockProducto>> get_ETL_stock_de_productos(string session, int tiSkip, string tsEMpresa)
        {
            string err = "";

            try
            {
                HttpClientHandler clientHandler = new HttpClientHandler();
                var cookieContainer = new CookieContainer();
                clientHandler.CookieContainer = cookieContainer;
                clientHandler.ServerCertificateCustomValidationCallback = (sender, cert, chain, sslPolicyErrors) => { return true; };
                using (var _client = new HttpClient(clientHandler))
                {
                    _client.BaseAddress = new Uri("https://119.8.155.113:50000/b1s/v1/sml.svc/");
                    cookieContainer.Add(_client.BaseAddress, new Cookie("B1SESSION", session));
                    _client.DefaultRequestHeaders.Accept.Clear();
                    _client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                    _client.DefaultRequestHeaders.Add("Prefer", "odata.maxpagesize=0");

                    var httpResponse = await _client.GetAsync("FU_STOCK_PRODUCTOS?$filter =Empresa eq '" + tsEMpresa + "'");
                    var responseContent = await httpResponse.Content.ReadAsStringAsync();
                    var respuesta = JsonConvert.DeserializeObject<ODataResponse<StockProducto>>(responseContent);
                    return respuesta.Value;
                }
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "get_ETL_stock_de_productos", e.Message);
                return null;
            }
        }


        private static async Task<int> SAP_OC_All_Estatus(string tsEMpresa)
        {
            string err = "";
            try
            {
                int tiSkip = 0;
                int cantSLayer = 0;
                int cantRegHANA = 0;
                List<OC_Status> OC_Status = new List<OC_Status>();
                OC_Status = await get_ETL_Oc_All_Status(session, tiSkip, tsEMpresa);
                OC_StatusALL.AddRange(OC_Status);
                cantSLayer = OC_Status.Count;
                cantRegHANA = OC_StatusALL.Count;
                return cantRegHANA;
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "SAP_OC_All_Estatus", e.Message);
                Console.WriteLine("ERROR -->" + err + " " + tiSkip);
                return -1;
            }
        }
        public static async Task<List<OC_Status>> get_ETL_Oc_All_Status(string session, int tiSkip, string tsEMpresa)
        {
            string err = "";

            try
            {
                HttpClientHandler clientHandler = new HttpClientHandler();
                var cookieContainer = new CookieContainer();
                clientHandler.CookieContainer = cookieContainer;
                clientHandler.ServerCertificateCustomValidationCallback = (sender, cert, chain, sslPolicyErrors) => { return true; };
                using (var _client = new HttpClient(clientHandler))
                {
                    _client.BaseAddress = new Uri("https://119.8.155.113:50000/b1s/v1/sml.svc/");
                    cookieContainer.Add(_client.BaseAddress, new Cookie("B1SESSION", session));
                    _client.DefaultRequestHeaders.Accept.Clear();
                    _client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                    _client.DefaultRequestHeaders.Add("Prefer", "odata.maxpagesize=0");

                    //var httpResponse = await _client.GetAsync("FU_OC_ALL_STATUS");
                    var httpResponse = await _client.GetAsync("FU_OC_ALL_STATUS?$filter =Empresa eq '" + tsEMpresa + "'");
                    var responseContent = await httpResponse.Content.ReadAsStringAsync();
                    var respuesta = JsonConvert.DeserializeObject<ODataResponse<OC_Status>>(responseContent);

                    return respuesta.Value;
                }
            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error(tsEMpresa, 1, "get_ETL_Oc_All_Status", e.Message);
                return null;
            }
        }





        //LOGIN SAP
        private static async Task<string> LoginSAP()
        {
            string err = "";
            var b1Session = new B1Session();
            var crendenciales = new Credenciales("manager", "Msap21", "CLTST_VIELVA");//CLTST_VIELVA2 //123456
            try
            {

                HttpClientHandler clientHandler = new HttpClientHandler();
                clientHandler.ServerCertificateCustomValidationCallback = (sender, cert, chain, sslPolicyErrors) => { return true; };

                using (var _client = new HttpClient(clientHandler))
                {

                    var myContent = JsonConvert.SerializeObject(crendenciales);
                    _client.BaseAddress = new Uri(_sl);
                    _client.DefaultRequestHeaders.Accept.Clear();
                    _client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                    HttpResponseMessage response = await _client.PostAsync("Login", new StringContent(myContent, Encoding.UTF8, "application/json"));

                    if (response.IsSuccessStatusCode)
                    {
                        var result = await response.Content.ReadAsStringAsync();
                        b1Session = JsonConvert.DeserializeObject<B1Session>(result);
                        return b1Session.SessionId;
                    }
                    else
                    {
                        return "";
                    }

                }

            }
            catch (Exception e)
            {
                err = e.Message;
                PBI.clsMaestroProductos.registrar_error("TODAS", 1, "LoginSAP", e.Message);
                Console.WriteLine("ERROR -->" + err);
                return "-1";
            }
        }

    }



}
