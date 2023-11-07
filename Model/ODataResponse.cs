using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETL.Model
{
    internal class ODataResponse<T>
    {
        public List<T> Value { get; set; }
    }


    public class LibroMayor
    {
        public string Country { get; set; }
        public string Company { get; set; }
        public int TransId { get; set; }
        public DateTime RefDate { get; set; }
        public string TransType { get; set; }
        public string BaseRef { get; set; }
        public string Folio { get; set; }
        public string LineMemo { get; set; }
        public string FormatCode { get; set; }
        public string AcctName { get; set; }
        public decimal DebitCredit { get; set; }
        public string OcrCode1 { get; set; }
        public string OcrCode2 { get; set; }
        public string OcrCode3 { get; set; }
        public string ContraAct { get; set; }
        public string ContraActName { get; set; }

        public int id__ { get; set; }
    }


    public class Tarjetas
    {
        public string itemCode { get; set; }
        public string U_Kit_Minero { get; set; }
        public string U_Acti_Vinc { get; set; }
        public string status { get; set; }
        public string internalSN { get; set; }

        public int ItmsGrpCod { get; set; }
    }

    public class Vendedor
    {
        public int SlpCode { get; set; }
        public string SlpName { get; set; }
        public string Memo { get; set; }
        public string Active { get; set; }
        public string U_Equipo { get; set; }
        public string U_VendedorComision { get; set; }
        public string U_Sucursal { get; set; }
        public string U_Area { get; set; }
        public string U_Gerente { get; set; }
        public string U_GruArtPer { get; set; }
        public string U_Tipo { get; set; }
        public string U_Autorizador { get; set; }
        public int CNT { get; set; }
        public string Empresa { get; set; }
    }

    public class SocNegocio
    {
        public string CardCode { get; set; }
        public string CardName { get; set; }
        public string LicTradNum { get; set; }
        public string CardType { get; set; }
        public string TipoSN { get; set; }
        public string Cuenta { get; set; }
        public string DesCuenta { get; set; }
        public string Currency { get; set; }
        public int GroupCode { get; set; }
        public string DesGrupo { get; set; }
        public string U_SecFinanciero { get; set; }
        public string U_SUBSEGMENTO { get; set; }
        public int SlpCode { get; set; }
        public string DesVendedor { get; set; }
        public int ListNum { get; set; }
        public string ListaPrecio { get; set; }
        public int GroupNum { get; set; }
        public string DesFPago { get; set; }
        public decimal CreditLine { get; set; }
        public string Address { get; set; }
        public string County { get; set; }
        public string City { get; set; }
        public string Country { get; set; }
        public string Propiedades { get; set; }
        public string Empresa { get; set; }
        public string Fecha_Actualizacion { get; set; }
        public string Fecha_Creacion { get; set; }

        public int id__ { get; set; }
    }

    public class Pallets
    {
        public string ItemCode { get; set; }
        public string ItemName { get; set; }
        public string U_Nivel_Maximo { get; set; }
        public string BinCode { get; set; }
        public string Ubicacion { get; set; }
        public string Nivel_Actual { get; set; }
        public string WhsCode { get; set; }
        public float Cant_en_Ubic { get; set; }
        public int U_NUniXpallet { get; set; }
        public float Pallets_en_Ubic { get; set; }
    }

    public class Articulos
    {
        public string ItemCode { get; set; }
        public string ItemName{get; set;}
        public string ItmsGrpNam { get; set; }
        public string CodeBars { get; set; }
        public float OnHand { get; set; }
        public string CardCode { get; set; }
        public float SHeight1 { get; set; }
        public int SWdth1Unit { get; set; }
        public float SLength1 { get; set; }
        public float Volumen_vta { get; set; }
        public float SWeight1 { get; set; }
        public float BHeight1 { get; set; }
        public float BWidth1 { get; set; }
        public float BLength1 { get; set; }
        public float BVolume { get; set; }
        public float BWeight1 { get; set; }
        public string U_Nivel_Maximo { get; set; }
        public int U_NUniXpallet { get; set; }
    }

    public class MaestoBodega
    {
        public string WhsCode { get; set; }
        public string WhsName { get; set; }
        public string City { get; set; }
        public int ctn { get; set; }
        public string Empresa { get; set; }

    }

    
    public class GuiaFactura
    {
        public string DocNum { get; set; }

        public string FolioNum { get; set; }

        public string U_NXIndTras { get; set; }

        public string DocDate { get; set; }

        public string DocNum_OINV { get; set; }

        public string FolioNum_OINV { get; set; }

        public string DocDate_OINV { get; set; }

        public string Empresa { get; set; }
    }
    
    public class EntradaMercaderia
    {
        public int DocNum { get; set; }
        public string CANCELED { get; set; }
        public string DocStatus { get; set; }
        public DateTime DocDate { get; set; }
        public string CardCode { get; set; }
        public string CardName { get; set; }
        public string NumAtCard { get; set; }
        public float DocRate { get; set; }
        public int SlpCode { get; set; }
        public int LineNum { get; set; }
        public string BaseRef { get; set; }
        public int BaseType { get; set; }
        public string LineStatus { get; set; }
        public string ItemCode { get; set; }
        public string Dscription { get; set; }
        public float Quantity { get; set; }
        public float OpenQty { get; set; }
        public float Price { get; set; }
        public string Currency { get; set; }
        public float Rate { get; set; }
        public float LineTotal { get; set; }
        public string OcrCode { get; set; }
    }

    public class PropiedadesProductos
    {
        public string ItemCode { get; set; }
        public string QryGroup1 { get; set; }
        public string QryGroup2 { get; set; }
        public string QryGroup4 { get; set; }
        public string QryGroup3 { get; set; }
        public string QryGroup5 { get; set; }
        public string QryGroup6 { get; set; }
        public string QryGroup7 { get; set; }
        public string QryGroup8 { get; set; }
        public string QryGroup9 { get; set; }
        public string QryGroup10 { get; set; }
        public string QryGroup11 { get; set; }
        public string QryGroup12 { get; set; }
        public string QryGroup13 { get; set; }
        public string QryGroup14 { get; set; }
        public string QryGroup15 { get; set; }
        public string QryGroup16 { get; set; }
        public string QryGroup17 { get; set; }
        public string QryGroup18 { get; set; }
    }

    public class NombresPropiedades
    {
        public int ItmsTypCod { get; set; }
        public string ItmsGrpNam { get; set; }
    }
    public class Productos
    {
        public string Id_Articulo { get; set; }
        public string Descripcion { get; set; }
        public string ArtInventario { get; set; }
        public string ArtVenta { get; set; }
        public string ArtCompra { get; set; }
        public string GrupoArticulo { get; set; }
        public string Activo { get; set; }
        public string Inactivo { get; set; }
        public string TipListaMaterial { get; set; }
        public string Propiedades { get; set; }
        public string U_Familia { get; set; }
        public string U_Subfamilia { get; set; }
        public string U_SubSubfamilia { get; set; }
        public string Clasif_Comercial { get; set; }
        public string Empresa { get; set; }
        public decimal IdGrupoArticulo { get; set; }
        public decimal CostoArticulo { get; set; }
        public string CardCode { get; set; }
        public string LastPurDat { get; set; }
        public string UpdateDate { get; set; }
        public int id__ { get; set; }
    }

    public class ListaPrecio
    {
        public string Numero_Articulo { get; set; }
        public string Descripcion { get; set; }
        public decimal Precio { get; set; }
        public string Empresa { get; set; }
        public int id__ { get; set; }
    }

    public class StockProducto
    {
        public string ItemCode { get; set; }
        public string ItemName { get; set; }
        public string WhsCode { get; set; }
        public string WhsName { get; set; }
        public decimal OnHand { get; set; }
        public decimal IsCommited { get; set; }
        public decimal OnOrder { get; set; }
        public string Empresa { get; set; }
        public int id__ { get; set; }
    }

    public class OC_Status
    {
        public string CardCode { get; set; }
        public string CardName { get; set; }
        public int DocNum { get; set; }
        public DateTime DocDate { get; set; }
        public string ItemCode { get; set; }
        public string Dscription { get; set; }
        public string Currency { get; set; }
        public decimal Quantity { get; set; }
        public decimal Can_Recibida { get; set; }
        public decimal Cant_Abierta { get; set; }
        public decimal Price { get; set; }
        public decimal Total { get; set; }
        public decimal OpenSum { get; set; }
        public string WhsCode { get; set; }
        public string TaxCode { get; set; }
        public decimal Quantitys { get; set; }
        public string Comentario { get; set; }
        public string U_NAME { get; set; }
        public string CANCELED { get; set; }
        public string DocStatus { get; set; }
        public string DocType { get; set; }
        public string BaseRef { get; set; }
        public string LineNum { get; set; }
        public string LineStatus { get; set; }
        public string Fecha_ETD { get; set; }
        public string Fecha_ETA { get; set; }
        public string NumAtCard { get; set; }
        public string Empresa { get; set; }
        public int id__ { get; set; }
    }

    public class DetalleVtasDO
    {
        public DateTime DocDate { get; set; }
        public string OcrCode { get; set; }
        public int GroupCode { get; set; }
        public string GroupName { get; set; }
        public string CardCode { get; set; }
        public string CardName { get; set; }
        public int FolioNum { get; set; }
        public int ItmsGrpCod { get; set; }
        public string ItmsGrpNam { get; set; }
        public string Bodega { get; set; }
        public string U_Familia { get; set; }
        public string ItemCode { get; set; }
        public string Dscription { get; set; }
        public decimal Quantity { get; set; }
        public decimal Price { get; set; }
        public decimal Costo { get; set; }
        public decimal Descuento { get; set; }
        public int CodVen_Factura { get; set; }
        public string Vendedor { get; set; }
        public string Despacho_A { get; set; }
        public string Vinculada { get; set; }
        public string PymntGroup { get; set; }
        public int DocNum { get; set; }
        public string FolioPref { get; set; }
        public string OcrCode2 { get; set; }
        public string OcrCode3 { get; set; }
        public string Empresa { get; set; }
        public string DocCur { get; set; }
        public decimal DocRate { get; set; }
        public decimal DiscPrcnt { get; set; }
        public decimal Rate { get; set; }

        public decimal CostoLinea { get; set; }
        public string Activo { get; set; }
        public string ID_CRM { get; set; }
        public string Tipo_Doc { get; set; }
        public string Status { get; set; }
        public string CANCELED { get; set; }
        public string JrnlMemo { get; set; }
        public string Sala_de_Ventas { get; set; }
        public string U_Canal { get; set; }
        public string U_MotivoNC { get; set; }
        public string U_TipoNC { get; set; }
        public string U_NCRefacturacion { get; set; }



        public int id__ { get; set; }
    }


    public class SSTTrep
    {
        public int OrdenTrabajo { get; set; }
        public string Estado { get; set; }
        public string Prioridad { get; set; }
        public string Cliente { get; set; }
        public string NombreCliente { get; set; }
        public string SKU { get; set; }
        public string NombreSKU { get; set; }
        public string NumeroSerie { get; set; }
        public string NumeroSerieFabricante { get; set; }
        public DateTime cntrctDate { get; set; }
        public string Origen { get; set; }
        public string TipoProblema { get; set; }
        public string TipoOT { get; set; }
        public string DocIngreso { get; set; }
        public string Comentario { get; set; }
        public string Resolucion { get; set; }
        public DateTime createDate { get; set; }
        public DateTime closeDate { get; set; }
        public int contctCode { get; set; }
        public string subject { get; set; }
        public string NombreTecnico { get; set; }
        public string ApellidoTecnico { get; set; }
        public int SubTipoProblema { get; set; }
        public int DocNum { get; set; }

        public string Sucursal { get; set; }
        public string U_Horometro { get; set; }
        public string U_CodigoCliente { get; set; }
        public string U_FechaHorometro { get; set; }
        public string U_Emergencias { get; set; }
        public string U_Ubicacion_Equipo { get; set; }
        public string U_Estado_Equipo { get; set; }
        public string U_Recl_Fabr { get; set; }
        public string U_Fabr_Res_Gar { get; set; }
        public string U_Gestion_Gtia_prov { get; set; }
        public DateTime updateDate { get; set; }
        public string U_PM_Resp { get; set; }
        public string U_Fecha_g_Gti_Prov { get; set; }
        public string U_Recla_Fabr { get; set; }
        public string U_Fe_Fab_res_Gtia { get; set; }
        public string U_F_Recp_Rep_Gtia { get; set; }
        public string Empresa { get; set; }
        public int id__ { get; set; }
    }
    public class NVtaDig
    {
        public int DocNum { get; set; }
        public string CardCode { get; set; }
        public string CardName { get; set; }
        public DateTime DocDate { get; set; }
        public DateTime FechaEntrega { get; set; }
        public string ClaseExpedicion { get; set; }
        public string Situacion { get; set; }
        public string OC { get; set; }
        public string ItemCode { get; set; }
        public string Dscription { get; set; }
        public string WhsCode { get; set; }
        public decimal PriceBefDi { get; set; }
        public decimal Price { get; set; }
        public decimal Quantity { get; set; }
        public decimal DelivrdQty { get; set; }
        public decimal LineTotal { get; set; }
        public decimal PorcCumplimiento { get; set; }
        public int SlpCode { get; set; }
        public string SlpName { get; set; }
        public string OcrCode { get; set; }
        public string OcrCode2 { get; set; }
        public string OcrCode3 { get; set; }
        public string U_TipoOV { get; set; }
        public string Empresa { get; set; }
        public int id__ { get; set; }

    }
    public class PedidoBloq
    {
        public string ObjType { get; set; }
        public int DoctoPreliminar { get; set; }
        public int NumeroDocumento { get; set; }
        public string Estado { get; set; }
        public string WddStatus { get; set; }
        public string ClaseDocumento { get; set; }
        public string Cancelada { get; set; }
        public string Estados { get; set; }
        public string Documento { get; set; }
        public DateTime DocDate { get; set; }
        public DateTime DocDueDate { get; set; }
        public string CardCode { get; set; }
        public string CardName { get; set; }
        public decimal DiscPrcnt { get; set; }
        public decimal DocTotal { get; set; }
        public string DocCur { get; set; }
        public string Comments { get; set; }
        public decimal DocRate { get; set; }
        public string ItemCode { get; set; }
        public string Dscription { get; set; }
        public decimal Quantity { get; set; }
        public DateTime ShipDate { get; set; }
        public string Currency { get; set; }
        public decimal Precio_Unidad { get; set; }
        public decimal Descuento { get; set; }
        public decimal PrecioTrasDesco { get; set; }
        public decimal LineTotal { get; set; }
        public string e_aPROB { get; set; }
        public string Status { get; set; }
        public int SlpCode { get; set; }
        public int LineNum { get; set; }
        public string Empresa { get; set; }
        public int id__ { get; set; }
    }

    public class PartidasAbiertas
    {
        public int N_Documento { get; set; }
        public string N_Folio { get; set; }
        public string Rut_Cliente { get; set; }
        public string Nombre_Cliente { get; set; }
        public int Clase_Doc_Destino { get; set; }
        public int Clase_Doc_Base { get; set; }
        public string Moneda { get; set; }
        public DateTime Fecha_Vencimiento { get; set; }
        public DateTime Fecha_Contabilizacion { get; set; }
        public DateTime Fecha_Documento { get; set; }
        public string Destino { get; set; }
        public string N_articulo { get; set; }
        public string Descripcion_articulo { get; set; }
        public decimal Cantidad { get; set; }
        public decimal Precio_por_unidad { get; set; }
        public decimal Porc_descuento { get; set; }
        public string Indicador_impuestos { get; set; }
        public decimal Total_ML { get; set; }
        public string Almacen { get; set; }
        public string Empleado_ventas { get; set; }
        public decimal En_Stock { get; set; }
        public decimal Comprometido { get; set; }
        public string Direccion { get; set; }
        public string Detalles_de_articulo { get; set; }
        public string Propietario { get; set; }
        public string Nombre_destinatario { get; set; }
        public string Descripcion_destinatario { get; set; }
        public string Norma_de_reparto { get; set; }
        public string Norma_reparto_precios_coste { get; set; }
        public string Codigo_u_medida { get; set; }
        public string U_NXIndTras { get; set; }
        
        public string Empresa { get; set; }
        public int id__ { get; set; }
    }

    public class FacturaReserv
    {
        public int DocEntry { get; set; }
        public int DocNum { get; set; }
        public char DocType { get; set; }
        public string CardCode { get; set; }
        public string CardName { get; set; }
        public string Estado { get; set; }
        public string Id_Producto { get; set; }
        public string Producto { get; set; }
        public string Dscription { get; set; }
        public decimal Cant { get; set; }
        public decimal Precio { get; set; }
        public decimal Total_LInea { get; set; }
        public string Moneda { get; set; }
        public string BaseRef { get; set; }
        public string U_Fecha_ETA { get; set; }
        public string NumAtCard { get; set; }
        public decimal Cant_Pendiente { get; set; }
        public string isIns { get; set; }

        public string Fecha { get; set; }
        public string ETD { get; set; }

        public string Empresa { get; set; }
        public int id__ { get; set; }
    }


    public class OfertaVta
    {

        public int DocNum { get; set; }
        public string DocType { get; set; }
        public string CANCELED { get; set; }
        public string DocStatus { get; set; }
        public string ObjType { get; set; }
        public DateTime DocDate { get; set; }
        public string CardCode { get; set; }
        public string CardName { get; set; }
        public string DocCur { get; set; }
        public decimal DocRate { get; set; }
        public int DocEntry { get; set; }
        public int LineNum { get; set; }
        public int TargetType { get; set; }
        public string LineStatus { get; set; }
        public string ItemCode { get; set; }
        public string Dscription { get; set; }
        public decimal Quantity { get; set; }
        public decimal OpenQty { get; set; }
        public decimal Price { get; set; }
        public string Currency { get; set; }
        public decimal Rate { get; set; }
        public decimal LineTotal { get; set; }
        public decimal OpenSum { get; set; }
        public string Fecha_ETA { get; set; }
        public string Fecha_ETD { get; set; }
        public string Ref_Acreedor { get; set; }
        public string Empresa { get; set; }

        public int id__ { get; set; }
    }







}