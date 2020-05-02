package co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.jpa;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.NoResultException;
import javax.persistence.Query;

import org.joda.time.DateTime;

import co.gov.ugpp.parafiscales.servicios.liquidador.entity.AportanteLIQ;
import co.gov.ugpp.parafiscales.servicios.liquidador.entity.CobFlex;
import co.gov.ugpp.parafiscales.servicios.liquidador.entity.HojaCalculoLiquidacionDetalle;
import co.gov.ugpp.parafiscales.servicios.liquidador.entity.Nomina;
import co.gov.ugpp.parafiscales.servicios.liquidador.entity.NominaDetalle;
import co.gov.ugpp.parafiscales.servicios.liquidador.entity.PilaDepurada;
import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.DatosEjecucionRegla;
import javax.persistence.NonUniqueResultException;

public class GestorProgramaDao extends JPAEntityDao
{

   public GestorProgramaDao(EntityManager paramEntityManager)
   {
      super(paramEntityManager);
   }

   @SuppressWarnings("unchecked")
   public List<Nomina> nominaByIdExpediente(String idExpediente)
   {
      //String sqlNomina = "SELECT ID, NIT, RAZON_SOCIAL, ANO, MES, FECHACREACION FROM LIQ_NOMINA WHERE IDEXPEDIENTE = ? ORDER BY FECHACREACION DESC";
      String sqlNomina = "SELECT * FROM (SELECT ID, NIT, RAZON_SOCIAL, ANO, MES, FECHACREACION FROM LIQ_NOMINA WHERE IDEXPEDIENTE = ? ORDER BY FECHACREACION DESC) WHERE ROWNUM = 1";
      Query query = getEntityManager().createNativeQuery(sqlNomina, Nomina.class);
      query.setParameter(1, idExpediente);

      List<Nomina> list = query.getResultList();
      if (list != null && !list.isEmpty())
          return list;

      //return list;
      return null;
   }

   
   /*
   public CotizanteLIQ cotizanteByIdCotizante(NominaDetalle nominaDetalle)
   {
      String sql = "SELECT * FROM LIQ_COTIZANTE WHERE ID = ? ORDER BY FEC_CREACION DESC";

      Query query = getEntityManager().createNativeQuery(sql, CotizanteLIQ.class);
      query.setParameter(1, nominaDetalle.getIdcotizante().getId());

      List<CotizanteLIQ> list = query.getResultList();

      if (list != null && !list.isEmpty())
         return list.get(0);

      return null;
      
   }
   */

   
   
   public AportanteLIQ aportanteLIQById(NominaDetalle nominaDetalle)
   {
      String sql = "SELECT * FROM LIQ_APORTANTE WHERE ID = ? ORDER BY FEC_CREACION DESC";

      Query query = getEntityManager().createNativeQuery(sql, AportanteLIQ.class);
      query.setParameter(1, nominaDetalle.getIdaportante().getId());
 
      List<AportanteLIQ> list = query.getResultList();

      if (list != null && !list.isEmpty())
         return list.get(0);

      return null;
   }

   
   
   @SuppressWarnings("unchecked")
   public List<NominaDetalle> nominaDetalleByIdNomina(Nomina nomina)
   {
	   // CAMBIO REALIZADO PARA HACER DEBUG
      String sqlNominaDetalle = "SELECT * FROM LIQ_NOMINA_DETALLE WHERE NOMINA = ? ORDER BY IDCOTIZANTE, ANO, MES";
      //String sqlNominaDetalle = "SELECT * FROM LIQ_NOMINA_DETALLE WHERE NOMINA = ? AND IDCOTIZANTE= 197 AND ANO=2014 AND MES=9 ORDER BY IDCOTIZANTE, ANO, MES";

      Query query = getEntityManager().createNativeQuery(sqlNominaDetalle, NominaDetalle.class);
      query.setParameter(1, nomina.getId());

      List<NominaDetalle> list = query.getResultList();

      return list;
   }

   
   public BigDecimal obtenerEmpleadoPeriodo(NominaDetalle nominaDetalle)
   {
      try
      {
         String sql = "SELECT count(LND.idcotizante) AS CANTIDAD FROM liq_nomina_detalle LND"
                    +" WHERE LND.nomina = ? AND LND.ano = ? AND LND.mes = ? GROUP BY LND.nomina, LND.ano, LND.mes  ORDER BY LND.ano, LND.mes";

         Query query = getEntityManager().createNativeQuery(sql);
         query.setParameter(1, nominaDetalle.getNomina().getId());
         query.setParameter(2, nominaDetalle.getAno());
         query.setParameter(3, nominaDetalle.getMes());

         BigDecimal result = (BigDecimal) query.getSingleResult();

         return result;
       }catch (NoResultException e){
            return new BigDecimal("0");
       }
         
   }
   
   @SuppressWarnings("unchecked")
   public NominaDetalle nominaDetalleAnteriorByNominaDetalleMesAnterior(DatosEjecucionRegla obj)
   {
       
      DateTime dateTime = new DateTime(obj.getNominaDetalle().getAno().intValue(), obj.getNominaDetalle().getMes().intValue(),
            1, 5, 1);
      DateTime fechaAnterior = dateTime.minusMonths(1);

      String sqlNominaDetalle = "SELECT * FROM LIQ_NOMINA_DETALLE WHERE IDAPORTANTE = ? AND NUM_IDENTIFI_REALIZO_APORTES = ? AND TIPO_IDENTIFI_REALIZO_APORTES = ? AND ANO = ? AND MES = ? ORDER BY ID DESC";

      Query query = getEntityManager().createNativeQuery(sqlNominaDetalle, NominaDetalle.class);
      query.setParameter(1, obj.getNominaDetalle().getIdaportante().getId());
      
      query.setParameter(2, obj.getNominaDetalle().getNumeroIdentificacionActual());
      query.setParameter(3, obj.getNominaDetalle().getTipoNumeroIdentificacionActual());
      
      query.setParameter(4, fechaAnterior.getYear());
      query.setParameter(5, fechaAnterior.getMonthOfYear());

      List<NominaDetalle> list = query.getResultList();

      if (list != null && !list.isEmpty())
         return list.get(0);

      return null;
   }

   @SuppressWarnings("unchecked")
   public HojaCalculoLiquidacionDetalle obtenerHojaCalculoLiquidacionDetalle(Nomina nomina, NominaDetalle nominaDetalle)
   {
      String sql = "SELECT ID,IBC_CALCULADO_SALUD FROM HOJA_CALCULO_LIQUIDACION_DETAL WHERE APORTA_NUMERO_IDENTIFICACION = ? "
            + "AND COTIZ_NUMERO_IDENTIFICACION = ? AND COTIZD_ANO = ? AND COTIZD_MES = ? ORDER BY IDHOJACALCULOLIQUIDACION DESC";

      // obtener año y mes anterior para hacer filtro
      DateTime dateTime = new DateTime(nominaDetalle.getAno().intValue(), nominaDetalle.getMes().intValue(), 1, 5, 1);
      DateTime fechaAnterior = dateTime.minusMonths(1);

      Query query = getEntityManager().createNativeQuery(sql, HojaCalculoLiquidacionDetalle.class);
      query.setParameter(1, nomina.getNit());
      query.setParameter(2, nominaDetalle.getNumeroIdentificacionActual());
      query.setParameter(3, fechaAnterior.getYear());// año
      query.setParameter(4, fechaAnterior.getMonthOfYear());// mes

      List<HojaCalculoLiquidacionDetalle> list = query.getResultList();

      if (list != null && !list.isEmpty())
         return list.get(0);

      return null;

   }



   
   @SuppressWarnings("unchecked")
   public HojaCalculoLiquidacionDetalle obtenerHojaCalculoLiqDetalleMesAnterior(Nomina nomina, NominaDetalle nominaDetalle, String idHojaCalculoLiquidacion)
   {
      String sql = "SELECT ID,COD_ADM_SALUD, COD_ADM_PENSION, COD_ADM_ARL, COD_ADM_CCF FROM HOJA_CALCULO_LIQUIDACION_DETAL WHERE IDHOJACALCULOLIQUIDACION = ? "
            + "AND APORTA_NUMERO_IDENTIFICACION = ? AND COTIZD_ANO = ? AND COTIZD_MES = ? AND COTIZ_NUMERO_IDENTIFICACION = ? ORDER BY IDHOJACALCULOLIQUIDACION DESC";

      // obtener año y mes anterior para hacer filtro
      DateTime dateTime = new DateTime(nominaDetalle.getAno().intValue(), nominaDetalle.getMes().intValue(), 1, 5, 1);
      DateTime fechaAnterior = dateTime.minusMonths(1);

      Query query = getEntityManager().createNativeQuery(sql, HojaCalculoLiquidacionDetalle.class);
      query.setParameter(1, idHojaCalculoLiquidacion);// idHojaCalculoLiquidacion
      query.setParameter(2, nomina.getNit());
      query.setParameter(3, fechaAnterior.getYear());// año
      query.setParameter(4, fechaAnterior.getMonthOfYear());// mes
      query.setParameter(5, nominaDetalle.getNumeroIdentificacionActual());

      List<HojaCalculoLiquidacionDetalle> list = query.getResultList();

      
      if (list != null && !list.isEmpty())
         return list.get(0);
      

      return null;

   }

   
   
   
   
   
   
   
   public HojaCalculoLiquidacionDetalle obtenerHojaCalculoLiqDetalleDeNominaDetalle(NominaDetalle nominaDetalle)
   {

      String sql = "SELECT * FROM HOJA_CALCULO_LIQUIDACION_DETAL WHERE IDNOMINADETALLE = ? ORDER BY IDHOJACALCULOLIQUIDACION DESC";

      Query query = getEntityManager().createNativeQuery(sql, HojaCalculoLiquidacionDetalle.class);
      query.setParameter(1, nominaDetalle.getId());

      try
      {
         HojaCalculoLiquidacionDetalle result = (HojaCalculoLiquidacionDetalle) query.getSingleResult();
         return result;
      }
      catch (NonUniqueResultException nure)
      {
          //System.out.println("::obtenerHojaCalculoLiqDetalleDeNominaDetalle:: NonUniqueResultException, se devolvio el primer registro. " + nure.getMessage()); 
          return (HojaCalculoLiquidacionDetalle)query.getResultList().get(0);
      }  
      catch (Exception ex)
      {
         //System.out.println("::obtenerHojaCalculoLiqDetalleDeNominaDetalle:: Exception, posible ningun registro. Se devolvio null." + ex.getMessage());  
         return null;
      }  
      

   
   }
   
   
   
   
   
    public NominaDetalle obtenerNominaDetalleAnteriorInicioVacacionesMismoAno(NominaDetalle nominaDetalle) {
        
      //  SELECT * FROM LIQ_NOMINA_DETALLE WHERE  NOMINA = ? AND DIAS_VACACIONES_MES = '0' AND MES < ? ORDER BY MES DESC
      String sql = "SELECT * FROM LIQ_NOMINA_DETALLE WHERE  NOMINA = ? AND DIAS_VACACIONES_MES = '0' AND MES < ? AND ANO = ? AND NUM_IDENTIFI_REALIZO_APORTES = ? AND TIPO_IDENTIFI_REALIZO_APORTES = ? ORDER BY MES DESC";

      Query query = getEntityManager().createNativeQuery(sql, NominaDetalle.class);
      query.setParameter(1, nominaDetalle.getNomina().getId());
      query.setParameter(2, nominaDetalle.getMes());
      query.setParameter(3, nominaDetalle.getAno());
      query.setParameter(4, nominaDetalle.getNumeroIdentificacionActual());
      query.setParameter(5, nominaDetalle.getTipoNumeroIdentificacionActual());
      
      try
      {
         NominaDetalle result = (NominaDetalle) query.getSingleResult();
         return result;
      } 
      catch (NonUniqueResultException nure)
      {
          //System.out.println("::obtenerNominaDetalleAnteriorDisfruteVacaciones:: NonUniqueResultException, se devolvio el primer registro. " + nure.getMessage()); 
          return (NominaDetalle)query.getResultList().get(0);
      }  
      catch (Exception ex)
      {
         //System.out.println("::obtenerNominaDetalleAnteriorDisfruteVacaciones:: Exception, posible ningun registro. Se devolvio null." + ex.getMessage());  
         return null;
      }    
    }
   
   
   
   
    public NominaDetalle obtenerNominaDetalleAnteriorInicioVacaciones(NominaDetalle nominaDetalle) {
        
      //  SELECT * FROM LIQ_NOMINA_DETALLE WHERE  NOMINA = ? AND DIAS_VACACIONES_MES = '0' AND MES < ? ORDER BY MES DESC
      String sql = "SELECT * FROM LIQ_NOMINA_DETALLE WHERE  NOMINA = ? AND DIAS_VACACIONES_MES = '0' AND MES < ? AND NUM_IDENTIFI_REALIZO_APORTES = ? AND TIPO_IDENTIFI_REALIZO_APORTES = ? ORDER BY MES DESC";

      Query query = getEntityManager().createNativeQuery(sql, NominaDetalle.class);
      query.setParameter(1, nominaDetalle.getNomina().getId());
      query.setParameter(2, nominaDetalle.getMes());
      query.setParameter(3, nominaDetalle.getNumeroIdentificacionActual());
      query.setParameter(4, nominaDetalle.getTipoNumeroIdentificacionActual());
      
      try
      {
         NominaDetalle result = (NominaDetalle) query.getSingleResult();
         return result;
      } 
      catch (NonUniqueResultException nure)
      {
          //System.out.println("::obtenerNominaDetalleAnteriorDisfruteVacaciones:: NonUniqueResultException, se devolvio el primer registro. " + nure.getMessage()); 
          return (NominaDetalle)query.getResultList().get(0);
      }  
      catch (Exception ex)
      {
         //System.out.println("::obtenerNominaDetalleAnteriorDisfruteVacaciones:: Exception, posible ningun registro. Se devolvio null." + ex.getMessage());  
         return null;
      }  
      
      
        
    }
   
   
   
   
   
   public NominaDetalle obtenerNominaDetalleInicioVacaciones(NominaDetalle nominaDetalle) {
        
      //  SELECT * FROM LIQ_NOMINA_DETALLE WHERE  NOMINA = ? AND DIAS_VACACIONES_MES = '0' AND MES < ? ORDER BY MES DESC
      String sql = "SELECT * FROM LIQ_NOMINA_DETALLE WHERE  NOMINA = ? AND DIAS_VACACIONES_MES > '0' AND MES < ? AND NUM_IDENTIFI_REALIZO_APORTES = ? AND TIPO_IDENTIFI_REALIZO_APORTES = ? ORDER BY MES ASC";

      Query query = getEntityManager().createNativeQuery(sql, NominaDetalle.class);
      query.setParameter(1, nominaDetalle.getNomina().getId());
      query.setParameter(2, nominaDetalle.getMes());
      query.setParameter(3, nominaDetalle.getNumeroIdentificacionActual());
      query.setParameter(4, nominaDetalle.getTipoNumeroIdentificacionActual());
      
      try
      {
         NominaDetalle result = (NominaDetalle) query.getSingleResult();
         return result;
      } 
      catch (NonUniqueResultException nure)
      {
          //System.out.println("::obtenerNominaAnteriorAnioAnterior:: NonUniqueResultException, se devolvio el primer registro. " + nure.getMessage()); 
          return (NominaDetalle)query.getResultList().get(0);
      }  
      catch (Exception ex)
      {
         //System.out.println("::obtenerNominaAnteriorAnioAnterior:: Exception, posible ningun registro. Se devolvio null." + ex.getMessage());  
         return null;
      }  
    }
   
   
   
   
   
   
      public BigDecimal sumarTotalDiasReportadosMesDobleLineaAnterior(NominaDetalle nominaDetalle) {

       try
       {
           //SELECT SUM(TOTAL_DIAS_REPORTADOS_MES) AS VALOR FROM LIQ_NOMINA_DETALLE WHERE NOMINA = '1977' AND ANO='2013' AND MES='1' AND IDCOTIZANTE='2366' GROUP BY TOTAL_DIAS_REPORTADOS_MES;
            String sql = "SELECT SUM(TOTAL_DIAS_REPORTADOS_MES) AS VALOR FROM LIQ_NOMINA_DETALLE WHERE NOMINA=? AND ANO=? AND MES=? AND IDCOTIZANTE=? GROUP BY TOTAL_DIAS_REPORTADOS_MES";
                
            //Query query = getEntityManager().createNativeQuery(sql);
            //query.setParameter(1, nominaDetalle.getId());
            
            /*
            falta meterle los parametros per le sql ya quedo cuadrado
                    
            y sacar el otro sql del ibc calculado salud        

            BigDecimal result = (BigDecimal) query.getSingleResult();
            */
            
            //return result;
            
            
            return new BigDecimal("1");
            
            
        }
        catch (NoResultException e)
        {
            return new BigDecimal("0");
        }
      
    }
   
 
      
    public BigDecimal sumarIbcCalculadoSaludMesDobleLineaAnterior(NominaDetalle nominaDetalle) 
    {

       try
       {
           //SELECT SUM(TOTAL_DIAS_REPORTADOS_MES) AS VALOR FROM LIQ_NOMINA_DETALLE WHERE NOMINA = '1977' AND ANO='2013' AND MES='1' AND IDCOTIZANTE='2366' GROUP BY TOTAL_DIAS_REPORTADOS_MES;
            String sql = "SELECT SUM(TOTAL_DIAS_REPORTADOS_MES) AS VALOR FROM LIQ_NOMINA_DETALLE WHERE NOMINA=? AND ANO=? AND MES=? AND IDCOTIZANTE=? GROUP BY TOTAL_DIAS_REPORTADOS_MES";
                
            //Query query = getEntityManager().createNativeQuery(sql);
            //query.setParameter(1, nominaDetalle.getId());
            
            /*
            falta meterle los parametros per le sql ya quedo cuadrado
                    
            y sacar el otro sql del ibc calculado salud        

            BigDecimal result = (BigDecimal) query.getSingleResult();
            */
            
            //return result;
            
            
            return new BigDecimal("1");
            
            
        }
        catch (NoResultException e)
        {
            return new BigDecimal("0");
        }
      
    }     
 
   
   
   
   
   
   
   
   
   
   
   
   
   /* no se utilizo porque traia el anterior id y no el actual por eso 
      se guardo en el infonegocio y se recupera de este.
    public BigDecimal obtenerHojaCalculoLiquidacionID(NominaDetalle nominaDetalle) {
       
        try
        {
            String sql = "SELECT MAX(IDHOJACALCULOLIQUIDACION) FROM HOJA_CALCULO_LIQUIDACION_DETAL WHERE IDNOMINADETALLE= ?";

            Query query = getEntityManager().createNativeQuery(sql);
            query.setParameter(1, nominaDetalle.getId());
            return (BigDecimal) query.getSingleResult();
        }
        catch (NoResultException e)
        {
            return new BigDecimal("0");
        }    
        
    }
   */
   
   
   
   
   
   public PilaDepurada obtenerPilaDepuradaMesAnterior(Nomina nomina, NominaDetalle nominaDetalle)
   {
      String sql = "SELECT ID,IBC_SALUD,DIAS_COT_SALUD,TARIFA_MAXIMA,codigo_EPS,codigo_arp,codigo_CCF FROM LIQ_PILA_DEPURADA WHERE NIT = ? "
            + "AND NUMERO_IDENTIFICACION = ? AND PERIODO_RESTO = ? ORDER BY ID DESC";

      // obtener año y mes anterior para hacer filtro
      DateTime dateTime = new DateTime(nominaDetalle.getAno().intValue(), nominaDetalle.getMes().intValue(), 1, 5, 1);
      DateTime fechaAnterior = dateTime.minusMonths(1);

      Query query = getEntityManager().createNativeQuery(sql, PilaDepurada.class);
      query.setParameter(1, nomina.getNit());
      query.setParameter(2, nominaDetalle.getNumeroIdentificacionActual());

      String sMes = "";

      if (fechaAnterior.getMonthOfYear() <= 9)
         sMes = "0" + fechaAnterior.getMonthOfYear();
      else
         sMes = "" + fechaAnterior.getMonthOfYear();

      query.setParameter(3, String.valueOf(fechaAnterior.getYear()) + sMes);

      try
      {
         PilaDepurada result = (PilaDepurada) query.getSingleResult();
         return result;
      }
      catch (NonUniqueResultException nure)
      {
          //System.out.println("::obtenerPilaDepuradaIBC:: NonUniqueResultException, se devolvio el primer registro. " + nure.getMessage()); 
          return (PilaDepurada)query.getResultList().get(0);
      }
      catch (Exception ex)
      {
        //System.out.println("::obtenerPilaDepuradaIBC:: Exception, posible ningun registro. Se devolvio null." + ex.getMessage());  
        return null;
      }

   }

   
   /*
   public CotizanteLIQ obtenerLiqCotizante(CotizanteLIQ cotizanteLIQ)
   {
      String sql = "SELECT ID,ACTIVIDAD_ALTO_RIESGO_PENSION FROM LIQ_COTIZANTE WHERE NUMERO_IDENTIFICACION = ? AND TIPO_IDENTIFI_REALIZO_APORTES = ?";

      Query query = getEntityManager().createNativeQuery(sql, CotizanteLIQ.class);
      query.setParameter(1, cotizanteLIQ.getNumeroIdentificacion());
      query.setParameter(2, cotizanteLIQ.getTipoNumeroIdentificacionRealizoAportes());
      
      try
      {
         CotizanteLIQ result = (CotizanteLIQ) query.getSingleResult();
         return result;
      }
      catch (Exception e)
      {
         //System.out.println("::obtenerLiqCotizante:: Exception, posible ningun registro. Se devolvio null." + e.getMessage());  
         return null;
      }

   }
   */
   
   
   
   
   

   @SuppressWarnings("unused")
   public BigDecimal sumaValorLiqConceptoContablePagoNoSalarial(Nomina nomina, NominaDetalle nominaDetalle)
   {

      try
      {
    	 // Modificación Febrero 12.2016
         //String sqlAportante = "SELECT ID FROM LIQ_APORTANTE WHERE NUMERO_IDENTIFICACION = ? AND TIPO_APORTANTE NOT IN ('5','6','8') "
          //     + "AND NATURALEZA_JURIDICA = '2'";

         String sqlAportante = "SELECT ID FROM LIQ_APORTANTE WHERE NUMERO_IDENTIFICACION = ? AND NATURALEZA_JURIDICA IN('1','2','3')";
         
         Query queryApr = getEntityManager().createNativeQuery(sqlAportante, AportanteLIQ.class);
         queryApr.setParameter(1, nomina.getNit().toString());

         AportanteLIQ aportante = (AportanteLIQ) queryApr.getSingleResult();

         if (aportante != null) {

             try
             {
                //String sql = "SELECT SUM(VALOR) AS VALOR FROM LIQ_CONCEPTO_CONTABLE WHERE IDNOMINADETALLE = ? "
                //      + "AND TIPO_PAGO_UGPP = 'TP NO SALARIAL' GROUP BY IDNOMINADETALLE";

                String sql = "SELECT SUM(VALOR) AS VALOR FROM LIQ_CONCEPTO_CONTABLE_DETALLE WHERE IDNOMINADETALLE = ? "
                        + "AND IDSUBSISTEMA=99 AND TIPO_PAGO_UGPP = 'TP NO SALARIAL' GROUP BY IDNOMINADETALLE";
                
                Query query = getEntityManager().createNativeQuery(sql);
                query.setParameter(1, nominaDetalle.getId());

                BigDecimal result = (BigDecimal) query.getSingleResult();

                return result;
             }
             catch (NoResultException e)
             {
                return new BigDecimal("0");
             }
         }
         
         return new BigDecimal("0");


      }
      catch (NoResultException e)
      {
         return new BigDecimal("0");
      }

   }

   //WROJAS - Trabajadores Independientes
   //Oct.31.2019 Nuevo Método
   public BigDecimal sumaValorLiqConceptoContableTotalIBCCalculadoSaludIngresoBruto(Nomina nomina, NominaDetalle nominaDetalle){
      try{
         try{
            String sql = "SELECT SUM(VALOR) AS VALOR FROM LIQ_CONCEPTO_CONTABLE_DETALLE WHERE IDNOMINADETALLE = " + nominaDetalle.getId() 
                    + " AND TIPO_PAGO_UGPP = 'INGRESO BRUTO' GROUP BY IDNOMINADETALLE";
            Query query = getEntityManager().createNativeQuery(sql);
            query.setParameter(1, nominaDetalle.getId());
            BigDecimal result = (BigDecimal) query.getSingleResult();

            return result;
         }catch (NoResultException e){
            return new BigDecimal("0");
         }
      }catch (NoResultException e){
         return new BigDecimal("0");
      }
   }
   
   
   public BigDecimal sumaValorLiqConceptoContableIbcPagosNomSaludNoTpIncapacidad(Nomina nomina, NominaDetalle nominaDetalle, String subSistema)
   {
      try
      {
         try
         {
        	 // Cambio de acceso a tabla LIQ_CONCEPTO_CONTABLE_DETALLE
            //String sql = "SELECT SUM(VALOR) AS VALOR FROM LIQ_CONCEPTO_CONTABLE WHERE IDNOMINADETALLE = ? "
            //      + "AND TIPO_PAGO_UGPP != 'TP INCAPACIDAD' AND SUBSISTEMAS LIKE '" + subSistema + "' GROUP BY IDNOMINADETALLE";
            
            //String sql = "SELECT SUM(VALOR) AS VALOR FROM LIQ_CONCEPTO_CONTABLE_DETALLE WHERE IDNOMINADETALLE = " + nominaDetalle.getId() 
            //        + " AND TIPO_PAGO_UGPP != 'TP INCAPACIDAD' AND IDSUBSISTEMA = "+ subSistema +" GROUP BY IDNOMINADETALLE";
            
            String sql = "SELECT SUM(VALOR) AS VALOR FROM LIQ_CONCEPTO_CONTABLE_DETALLE WHERE IDNOMINADETALLE = " + nominaDetalle.getId() 
                    + " AND ((TIPO_PAGO_UGPP != 'TP INCAPACIDAD' AND IDSUBSISTEMA = "+ subSistema +") OR ((TIPO_PAGO_UGPP = 'TP VACACIONES' OR TIPO_PAGO_UGPP = 'TP VACACIONES TERMINACION DE CONTRATO') AND IDSUBSISTEMA = "+ subSistema +")) GROUP BY IDNOMINADETALLE";
            
            //System.out.println("::ANDRES15:: SQL: " + sql);
            Query query = getEntityManager().createNativeQuery(sql);
            query.setParameter(1, nominaDetalle.getId());

            BigDecimal result = (BigDecimal) query.getSingleResult();

            return result;
         }
         catch (NoResultException e)
         {
            return new BigDecimal("0");
         }

      }
      catch (NoResultException e)
      {
         return new BigDecimal("0");
      }

   }

   
   /*
   public BigDecimal sumaValorLiqConceptoContableIbcPagosNomSaludNoTpIncapacidadVacaciones(Nomina nomina, NominaDetalle nominaDetalle, String subSistema)
   {
      try
      {
         try
         {
        	 // Cambio de acceso a tabla LIQ_CONCEPTO_CONTABLE_DETALLE
            //String sql = "SELECT SUM(VALOR) AS VALOR FROM LIQ_CONCEPTO_CONTABLE WHERE IDNOMINADETALLE = ? "
            //      + "AND TIPO_PAGO_UGPP != 'TP INCAPACIDAD' AND SUBSISTEMAS LIKE '" + subSistema + "' GROUP BY IDNOMINADETALLE";
            
            String sql = "SELECT SUM(VALOR) AS VALOR FROM LIQ_CONCEPTO_CONTABLE_DETALLE WHERE IDNOMINADETALLE = " + nominaDetalle.getId() 
                    + " AND (TIPO_PAGO_UGPP = 'TP VACACIONES' OR TIPO_PAGO_UGPP = 'TP VACACIONES TERMINACION DE CONTRATO') AND IDSUBSISTEMA = "+ subSistema +" GROUP BY IDNOMINADETALLE";
            
            
            System.out.println("::ANDRES15:: SQL2: " + sql);
            
            
            Query query = getEntityManager().createNativeQuery(sql);
            query.setParameter(1, nominaDetalle.getId());

            BigDecimal result = (BigDecimal) query.getSingleResult();

            return result;
         }
         catch (NoResultException e)
         {
            return new BigDecimal("0");
         }

      }
      catch (NoResultException e)
      {
         return new BigDecimal("0");
      }

   }
   */
   
   
   
  
   // se envian los subsistemas que deseen ser tenidos en cuenta en el calculo del valor
   //los demas subsistemas deben valer 0. Siempre se envian los 6 subsistemas.
   public BigDecimal sumaValorLiqConceptoContableIBC(NominaDetalle nominaDetalle, String subSistema1,
           String subSistema2, String subSistema3, String subSistema4, String subSistema5, String subSistema6)
   {
      try
      {
         try
         {
             
            //Cambio de acceso a tabla: LIQ_CONCEPTO_CONTABLE_DETALLE
            //String sql = "SELECT SUM(VALOR) AS VALOR FROM LIQ_CONCEPTO_CONTABLE WHERE IDNOMINADETALLE = ? "
            //  + "AND TIPO_PAGO_UGPP IN ('TP VACACIONES','TP VACACIONES TERMINACION DE CONTRATO') "
            //  + "AND SUBSISTEMAS LIKE '" + subSistema + "' GROUP BY IDNOMINADETALLE";

            //String sql = "SELECT SUM(VALOR) AS VALOR FROM LIQ_CONCEPTO_CONTABLE WHERE IDNOMINADETALLE = ? "
            //  + "AND TIPO_PAGO_UGPP IN ('TP VACACIONES','TP VACACIONES TERMINACION DE CONTRATO') "
            //  + "AND SUBSISTEMAS LIKE '" + subSistema + "' GROUP BY IDNOMINADETALLE";
            
            String subsistemas = "";
             
            if(!subSistema1.equals("0"))
                subsistemas = "IDSUBSISTEMA=1";
            
            
            if(!subSistema2.equals("0"))
            {
                if(subsistemas.equals(""))                
                    subsistemas = subsistemas + "IDSUBSISTEMA=2";
                else
                    subsistemas = subsistemas + " OR IDSUBSISTEMA=2";
            }
            
            if(!subSistema3.equals("0"))
            {
                if(subsistemas.equals(""))                
                    subsistemas = subsistemas + "IDSUBSISTEMA=3";
                else
                    subsistemas = subsistemas + " OR IDSUBSISTEMA=3";
            }
            
            if(!subSistema4.equals("0"))
            {
                if(subsistemas.equals(""))                
                    subsistemas = subsistemas + "IDSUBSISTEMA=4";
                else
                    subsistemas = subsistemas + " OR IDSUBSISTEMA=4";
            }
            
            if(!subSistema5.equals("0"))
            {
                if(subsistemas.equals(""))                
                    subsistemas = subsistemas + "IDSUBSISTEMA=5";
                else
                    subsistemas = subsistemas + " OR IDSUBSISTEMA=5";
            }
            
            if(!subSistema6.equals("0"))
            {
                if(subsistemas.equals(""))                
                    subsistemas = subsistemas + "IDSUBSISTEMA=6";
                else
                    subsistemas = subsistemas + " OR IDSUBSISTEMA=6";
            }
             
            
            //String sql = "SELECT SUM(VALOR) AS VALOR FROM LIQ_CONCEPTO_CONTABLE_DETALLE WHERE IDNOMINADETALLE = ? AND (?) GROUP BY IDNOMINADETALLE";                   
                    
            //String sql = "SELECT SUM(VALOR) AS VALOR FROM LIQ_CONCEPTO_CONTABLE_DETALLE WHERE IDNOMINADETALLE = " + nominaDetalle.getId() + " AND (" + subsistemas + ") GROUP BY IDNOMINADETALLE";
            
            
            String sql = "SELECT SUM(VALOR) AS VALOR FROM LIQ_CONCEPTO_CONTABLE_DETALLE WHERE IDNOMINADETALLE = " + 
                    nominaDetalle.getId() + " AND ((" + 
                    subsistemas + " AND TIPO_PAGO_UGPP != 'TP VACACIONES') AND (" + 
                    subsistemas + " AND TIPO_PAGO_UGPP != 'TP VACACIONES TERMINACION DE CONTRATO')) GROUP BY IDNOMINADETALLE";

            
            //String sql = "SELECT SUM(VALOR) AS VALOR FROM LIQ_CONCEPTO_CONTABLE_DETALLE WHERE IDNOMINADETALLE = ? AND (?) GROUP BY IDNOMINADETALLE"; 
            
            //System.out.println("::ANDRES4:: sql: " + sql);
            
            
            Query query = getEntityManager().createNativeQuery(sql);
            //query.setParameter(1, nominaDetalle.getId());
            //query.setParameter(2, subsistemas);


            BigDecimal result = (BigDecimal) query.getSingleResult();

            return result;
         }
         catch (NoResultException e)
         {
            return new BigDecimal("0");
         }

      }
      catch (NoResultException e)
      {
         return new BigDecimal("0");
      }

   }
   
   
 
   
   
   public BigDecimal sumaValorLiqConceptoContableIBC_CCF(NominaDetalle nominaDetalle, String subSistema1,
           String subSistema2, String subSistema3, String subSistema4, String subSistema5, String subSistema6)
   {
      try
      {
         try
         {
        	 // CAmbio de acceso a tabla: LIQ_CONCEPTO_CONTABLE_DETALLE
            //String sql = "SELECT SUM(VALOR) AS VALOR FROM LIQ_CONCEPTO_CONTABLE WHERE IDNOMINADETALLE = ? "
              //    + "AND TIPO_PAGO_UGPP IN ('TP VACACIONES','TP VACACIONES TERMINACION DE CONTRATO') "
                //  + "AND SUBSISTEMAS LIKE '" + subSistema + "' GROUP BY IDNOMINADETALLE";

            //String sql = "SELECT SUM(VALOR) AS VALOR FROM LIQ_CONCEPTO_CONTABLE WHERE IDNOMINADETALLE = ? "
                    //+ "AND TIPO_PAGO_UGPP IN ('TP VACACIONES','TP VACACIONES TERMINACION DE CONTRATO') "
              //      + "AND SUBSISTEMAS LIKE '" + subSistema + "' GROUP BY IDNOMINADETALLE";
            
            String subsistemas = "";
             
            if(!subSistema1.equals("0"))
                subsistemas = "IDSUBSISTEMA=1";
            
            
            if(!subSistema2.equals("0"))
            {
                if(subsistemas.equals(""))                
                    subsistemas = subsistemas + "IDSUBSISTEMA=2";
                else
                    subsistemas = subsistemas + " OR IDSUBSISTEMA=2";
            }
            
            if(!subSistema3.equals("0"))
            {
                if(subsistemas.equals(""))                
                    subsistemas = subsistemas + "IDSUBSISTEMA=3";
                else
                    subsistemas = subsistemas + " OR IDSUBSISTEMA=3";
            }
            
            if(!subSistema4.equals("0"))
            {
                if(subsistemas.equals(""))                
                    subsistemas = subsistemas + "IDSUBSISTEMA=4";
                else
                    subsistemas = subsistemas + " OR IDSUBSISTEMA=4";
            }
            
            if(!subSistema5.equals("0"))
            {
                if(subsistemas.equals(""))                
                    subsistemas = subsistemas + "IDSUBSISTEMA=5";
                else
                    subsistemas = subsistemas + " OR IDSUBSISTEMA=5";
            }
            
            if(!subSistema6.equals("0"))
            {
                if(subsistemas.equals(""))                
                    subsistemas = subsistemas + "IDSUBSISTEMA=6";
                else
                    subsistemas = subsistemas + " OR IDSUBSISTEMA=6";
            }
             
            
            //String sql = "SELECT SUM(VALOR) AS VALOR FROM LIQ_CONCEPTO_CONTABLE_DETALLE WHERE IDNOMINADETALLE = ? AND (?) GROUP BY IDNOMINADETALLE";                   
                    
            String sql = "SELECT SUM(VALOR) AS VALOR FROM LIQ_CONCEPTO_CONTABLE_DETALLE WHERE IDNOMINADETALLE = " + nominaDetalle.getId() + " AND (" + subsistemas + ") GROUP BY IDNOMINADETALLE";
            
            
            //String sql = "SELECT SUM(VALOR) AS VALOR FROM LIQ_CONCEPTO_CONTABLE_DETALLE WHERE IDNOMINADETALLE = ? AND (?) GROUP BY IDNOMINADETALLE"; 
            
            //System.out.println("::ANDRES4:: SELECT SUM(VALOR) AS VALOR FROM LIQ_CONCEPTO_CONTABLE_DETALLE WHERE IDNOMINADETALLE = " + 
            //        nominaDetalle.getId() +" AND ("+ subsistemas +") GROUP BY IDNOMINADETALLE");
            
            
            Query query = getEntityManager().createNativeQuery(sql);
            //query.setParameter(1, nominaDetalle.getId());
            //query.setParameter(2, subsistemas);


            BigDecimal result = (BigDecimal) query.getSingleResult();

            return result;
         }
         catch (NoResultException e)
         {
            return new BigDecimal("0");
         }

      }
      catch (NoResultException e)
      {
         return new BigDecimal("0");
      }

   }
   
   
   
   
   

   public BigDecimal sumaValorLiqConceptoContableNominaDetalle(Nomina nomina, NominaDetalle nominaDetalle, String subSistema)
   {
      try
      {
         try
         {
            //String sql = "SELECT SUM(VALOR) AS VALOR FROM LIQ_CONCEPTO_CONTABLE WHERE IDNOMINADETALLE = ? "
            //      + "AND SUBSISTEMAS LIKE '" + subSistema + "' GROUP BY IDNOMINADETALLE";

            String sql = "SELECT SUM(VALOR) AS VALOR FROM LIQ_CONCEPTO_CONTABLE_DETALLE WHERE IDNOMINADETALLE = ? "
                    + "AND IDSUBSISTEMA = "+ subSistema +" GROUP BY IDNOMINADETALLE";
            
            Query query = getEntityManager().createNativeQuery(sql);
            query.setParameter(1, nominaDetalle.getId());

            BigDecimal result = (BigDecimal) query.getSingleResult();

            return result;
         }
         catch (NoResultException e)
         {
            return new BigDecimal("0");
         }

      }
      catch (NoResultException e)
      {
         return new BigDecimal("0");
      }

   }

   public BigDecimal sumaValorLiqConceptoContableIbcPagosNomPension(Nomina nomina, NominaDetalle nominaDetalle)
   {
      try
      {
         try
         {
        	 // OJO: El subsistema es <1> para salud
//            String sql = "SELECT SUM(VALOR) AS VALOR FROM LIQ_CONCEPTO_CONTABLE WHERE IDNOMINADETALLE = ? "
//                  + " AND SUBSISTEMAS LIKE '%2%' AND TIPO_PAGO_UGPP = 'TP INCAPACIDAD' GROUP BY IDNOMINADETALLE";

            //String sql = "SELECT SUM(VALOR) AS VALOR FROM LIQ_CONCEPTO_CONTABLE WHERE IDNOMINADETALLE = ? "
            //        + " AND SUBSISTEMAS LIKE '%2%' AND TIPO_PAGO_UGPP = 'TP INCAPACIDAD' GROUP BY IDNOMINADETALLE";
            
            String sql = "SELECT SUM(VALOR) AS VALOR FROM LIQ_CONCEPTO_CONTABLE_DETALLE WHERE IDNOMINADETALLE = ? "
                    + " AND IDSUBSISTEMA=2 AND TIPO_PAGO_UGPP = 'TP INCAPACIDAD' GROUP BY IDNOMINADETALLE";
            
            Query query = getEntityManager().createNativeQuery(sql);
            query.setParameter(1, nominaDetalle.getId());

            BigDecimal result = (BigDecimal) query.getSingleResult();

            return result;
         }
         catch (NoResultException e)
         {
            return new BigDecimal("0");
         }

      }
      catch (NoResultException e)
      {
         return new BigDecimal("0");
      }

   }
   
   public BigDecimal sumaValorLiqConceptoContableIbcPagosNomSalud(Nomina nomina, NominaDetalle nominaDetalle)
   {
      try
      {
         try
         {
        	 // OJO: El subsistema es <1> para salud
             //String sql = "SELECT SUM(VALOR) AS VALOR FROM LIQ_CONCEPTO_CONTABLE WHERE IDNOMINADETALLE = ? "
             //     + " AND SUBSISTEMAS LIKE '1;%' AND TIPO_PAGO_UGPP = 'TP INCAPACIDAD' GROUP BY IDNOMINADETALLE";

            //String sql = "SELECT SUM(VALOR) AS VALOR FROM LIQ_CONCEPTO_CONTABLE WHERE IDNOMINADETALLE = ? "
            //        + " AND SUBSISTEMAS LIKE '1;%' AND TIPO_PAGO_UGPP = 'TP INCAPACIDAD' GROUP BY IDNOMINADETALLE";

            String sql = "SELECT SUM(VALOR) AS VALOR FROM LIQ_CONCEPTO_CONTABLE_DETALLE WHERE IDNOMINADETALLE = ? "
                    + " AND IDSUBSISTEMA=1 AND TIPO_PAGO_UGPP = 'TP INCAPACIDAD' GROUP BY IDNOMINADETALLE";            
            
            Query query = getEntityManager().createNativeQuery(sql);
            query.setParameter(1, nominaDetalle.getId());

            BigDecimal result = (BigDecimal) query.getSingleResult();

            return result;
         }
         catch (NoResultException e)
         {
            return new BigDecimal("0");
         }

      }
      catch (NoResultException e)
      {
         return new BigDecimal("0");
      }

   }

   public String observacionPension(NominaDetalle obj)
   {
      try
      {
         String sql = "SELECT OBSERVACIONES_PENSION FROM LIQ_APORTES_INDEPENDIENTE WHERE IDNOMINADETALLE = ?";

         Query query = getEntityManager().createNativeQuery(sql);
         query.setParameter(1, obj.getId());

         String result = (String) query.getSingleResult();

         return result;
      }
      catch (NoResultException e)
      {
         return null;
      }
   }
   
   
   
   
      public String observacionSalud(NominaDetalle obj)
   {
      try
      {
         String sql = "SELECT OBSERVACIONES_SALUD FROM LIQ_APORTES_INDEPENDIENTE WHERE IDNOMINADETALLE = ?";

         Query query = getEntityManager().createNativeQuery(sql);
         query.setParameter(1, obj.getId());

         String result = (String) query.getSingleResult();

         return result;
      }
      catch (NoResultException e)
      {
         return null;
      }
   }
   

      
    public String observacionArl(NominaDetalle obj)
    {
      try
      {
         String sql = "SELECT OBSERVACIONES_ARL FROM LIQ_APORTES_INDEPENDIENTE WHERE IDNOMINADETALLE = ?";

         Query query = getEntityManager().createNativeQuery(sql);
         query.setParameter(1, obj.getId());

         String result = (String) query.getSingleResult();

         return result;
      }
      catch (NoResultException e)
      {
         return null;
      }
   }
      
      
   
        public String observacionCCF(NominaDetalle obj)
    {
      try
      {
         String sql = "SELECT OBSERVACIONES_CCF FROM LIQ_APORTES_INDEPENDIENTE WHERE IDNOMINADETALLE = ?";

         Query query = getEntityManager().createNativeQuery(sql);
         query.setParameter(1, obj.getId());

         String result = (String) query.getSingleResult();

         return result;
      }
      catch (NoResultException e)
      {
         return null;
      }
   }
    
    
    
    
   
   public String obtenerNombreAdministradora(String codAdm)
   {
      try
      {
         String sql = "SELECT nombre  FROM ADMINISTRADORA_PILA  WHERE  CODIGO = ?";

         Query query = getEntityManager().createNativeQuery(sql);
         query.setParameter(1, codAdm);

         String result = (String) query.getSingleResult();

         return result;
      }
      catch (NoResultException e)
      {
         return null;
      }
   }
   
   public Map<String, String> getLstAdministradoraPila() {
       Map<String, String> lstResult = new HashMap<>();
       List<Object[]> lstTemp = null;
         String sql = "SELECT CODIGO, NOMBRE FROM ADMINISTRADORA_PILA";
         Query query = getEntityManager().createNativeQuery(sql);
         lstTemp = query.getResultList();
         if(lstTemp != null && !lstTemp.isEmpty()) {
             for(Object[] aux: lstTemp) {
                 lstResult.put(aux[0].toString(), aux[1].toString());
             }
         }
       
       return lstResult;
   }
   
   public String tieneSaludCotizante(NominaDetalle obj)
   {
      try
      {
         String sql = "SELECT TIENE_SALUD FROM LIQ_MALLA_VAL WHERE CODIGO_TIPO_COTIZANTE = ?";
         //System.out.println("::ANDRES901:: SELECT TIENE_SALUD FROM LIQ_MALLA_VAL WHERE CODIGO_TIPO_COTIZANTE = " + obj.getTipoCotizante());
         //System.out.println("::ANDRES902:: Integer = " + new Integer(obj.getTipoCotizante()));
         //System.out.println("::ANDRES903:: NominaDetalle = " + obj.getId());
         
         Query query = getEntityManager().createNativeQuery(sql);
         query.setParameter(1, new Integer(obj.getTipoCotizante()));

         String result = (String) query.getSingleResult();

         //System.out.println("Error EXCEPTION getTipoCotizante: " + obj.getTipoCotizante());
         //System.out.println("Error EXCEPTION result: " + result);
         
         return result;
      }
      catch (NoResultException e)
      {
         //System.out.println("Error EXCEPTION tieneSaludCotizante: " + e.getMessage());
         return null;
      }
   }
   
   public BigDecimal tipoIdentificacionAportante(NominaDetalle obj)
   {
      try
      {
         String sql = "SELECT TIPO_IDENTIFICACION FROM LIQ_APORTANTE WHERE ID = ?";

         Query query = getEntityManager().createNativeQuery(sql);
         query.setParameter(1, obj.getIdaportante().getId());

         BigDecimal result = (BigDecimal) query.getSingleResult();

         return result;
      }
      catch (NoResultException e)
      {
         return null;
      }
   }   

   // Octubre.04.2019 RQ: TrabajadoresIndependientes WROJAS
      public String tipoAportante(NominaDetalle obj){
      try{
         String sql = "SELECT TIPO_APORTANTE FROM LIQ_APORTANTE WHERE ID = ?";

         Query query = getEntityManager().createNativeQuery(sql);
         query.setParameter(1, obj.getIdaportante().getId());

         String result = (String) query.getSingleResult();

         return result;
      }catch (NoResultException e){
         return null;
      }
   }   
      
   public Map<String, String> obtenerMallaVal(NominaDetalle obj)
   {
      try
      {
         String sql = "SELECT * FROM LIQ_MALLA_VAL WHERE CODIGO_TIPO_COTIZANTE = ?";

         Query query = getEntityManager().createNativeQuery(sql);
         query.setParameter(1, new Integer(obj.getTipoCotizante()));

         Object[] result = (Object[]) query.getSingleResult();

         Map<String, String> resultMap = new HashMap<>();
         resultMap.put("DESCRIPCION", (String) result[1]);
         resultMap.put("TIENE_SALUD", (String) result[2]);
         resultMap.put("TIENE_PENSION", (String) result[3]);
         resultMap.put("ALTO_RIESGO", (String) result[4]);
         resultMap.put("TIENE_FSP", (String) result[5]);
         resultMap.put("TIENE_ARP", (String) result[6]);
         resultMap.put("TIENE_ICBF", (String) result[7]);
         resultMap.put("TIENE_SENA", (String) result[8]);
         resultMap.put("TIENE_CCF", (String) result[9]);

         return resultMap;
      }
      catch (NoResultException e)
      {
         return null;
      }
   }

   public String tienePension(NominaDetalle obj)
   {
      try
      {
         String sql = "SELECT APORTE_PENSION FROM LIQ_MALLA_VAL_STC WHERE COD_SUBCOTIZANTE = ?";

         Query query = getEntityManager().createNativeQuery(sql);
         query.setParameter(1, new Integer(obj.getSubTipoCotizante()));

         String result = (String) query.getSingleResult();

         return result;
      }
      catch (NoResultException e)
      {
         return null;
      }
   }

   public String tienePensionCotizante(NominaDetalle obj)
   {
      try
      {
         String sql = "SELECT TIENE_PENSION FROM LIQ_MALLA_VAL WHERE CODIGO_TIPO_COTIZANTE = ?";

         Query query = getEntityManager().createNativeQuery(sql);
         query.setParameter(1, new Integer(obj.getTipoCotizante()));

         String result = (String) query.getSingleResult();

         return result;
      }
      catch (NoResultException e)
      {
         return null;
      }
   }
   
   public BigDecimal valorSalarioMinimoMesLegal(Nomina nomina, NominaDetalle nominaDetalle)
   {
      try
      {
         try
         {
            String sql = "SELECT VALOR FROM COB_PARAM_GENERAL WHERE ID_PARAMETRO = ?";

            Query query = getEntityManager().createNativeQuery(sql);
            query.setParameter(1, "SMMLV" + nominaDetalle.getAno().toString());

            BigDecimal result = (BigDecimal) query.getSingleResult();

            return result;
         }
         catch (NoResultException e)
         {
            return new BigDecimal("0");
         }

      }
      catch (NoResultException e)
      {
         return new BigDecimal("0");
      }

   }

   public BigDecimal topeMaximoIbcSalud(Nomina nomina, NominaDetalle nominaDetalle)
   {

     String sql = "SELECT VALOR FROM COB_PARAM_GENERAL WHERE ID_PARAMETRO = ?";

      Query query = getEntityManager().createNativeQuery(sql);
      query.setParameter(1, "MAXIBCSALUD" + nominaDetalle.getAno().toString());

      BigDecimal result = (BigDecimal) query.getSingleResult();

	   
      return result;

   }

   public BigDecimal topeMaximoIbcPension(Nomina nomina, NominaDetalle nominaDetalle)
   {

      String sql = "SELECT VALOR FROM COB_PARAM_GENERAL WHERE ID_PARAMETRO = ?";

      Query query = getEntityManager().createNativeQuery(sql);
      query.setParameter(1, "MAXIBCPENSION" + nominaDetalle.getAno().toString());

      BigDecimal result = (BigDecimal) query.getSingleResult();

      return result;

   }

   /**
    * Filtro ID_SBSIS con ID 102,103
    * 
    * @param nomina
    * @param nominaDetalle
    * @return
    */
   public BigDecimal tarifaSalud(Nomina nomina, NominaDetalle nominaDetalle)
   {
        try
        {
          String sql = "SELECT SUM(PORCENTAJE_SBSIS) FROM COB_SBSIS WHERE ID_SBSIS IN (102,103)";
          Query query = getEntityManager().createNativeQuery(sql);
          BigDecimal result = (BigDecimal) query.getSingleResult();
          return result;
        }
        catch (Exception e)
        {
           return new BigDecimal("0");
        }
   }

   public BigDecimal tarifaSaludEmpleador(Nomina nomina, NominaDetalle nominaDetalle)
   {
      try
      {
        String sql = "SELECT SUM(PORCENTAJE_SBSIS) FROM COB_SBSIS WHERE ID_SBSIS IN (103)";
        Query query = getEntityManager().createNativeQuery(sql);
        BigDecimal result = (BigDecimal) query.getSingleResult();
        return result;
      }
      catch (Exception e)
      {
         return new BigDecimal("0");
      }
   }

   public BigDecimal tarifaCCF(Nomina nomina, NominaDetalle nominaDetalle)
   {
      try
      {
        String sql = "SELECT PORCENTAJE_SBSIS FROM COB_SBSIS WHERE ID_SBSIS = 194";
        Query query = getEntityManager().createNativeQuery(sql);
        BigDecimal result = (BigDecimal) query.getSingleResult();
        return result;
      }
      catch (Exception e)
      {
         return new BigDecimal("0");
      }

   }

   public BigDecimal tarifaSena(Nomina nomina, NominaDetalle nominaDetalle)
   {
        try
        {
           String sql = "SELECT PORCENTAJE_SBSIS FROM COB_SBSIS WHERE ID_SBSIS = 197";
           Query query = getEntityManager().createNativeQuery(sql);
           BigDecimal result = (BigDecimal) query.getSingleResult();
           return result;
        }
        catch (Exception e)
        {
            return new BigDecimal("0");
        }
   }

   public BigDecimal tarifaICBF(Nomina nomina, NominaDetalle nominaDetalle)
   {
        try
        {
           String sql = "SELECT PORCENTAJE_SBSIS FROM COB_SBSIS WHERE ID_SBSIS = 195";
           Query query = getEntityManager().createNativeQuery(sql);
           BigDecimal result = (BigDecimal) query.getSingleResult();
           return result;
        }
        catch (Exception e)
        {
            return new BigDecimal("0");
        }
   }

   public BigDecimal tarifaPensionPorcentual(Nomina nomina, NominaDetalle nominaDetalle)
   {
       try
       { 
            DateTime dateTime = new DateTime(nominaDetalle.getAno().intValue(), nominaDetalle.getMes().intValue(), 1, 0, 0);
            
            
            //System.out.println("::ANDRES6:: dateTime: " + dateTime.toDate());


            String sql = "SELECT PORCENTAJE_SBSIS FROM COB_SBSIS WHERE ID_SBSIS IN (105,106,238) AND ? >= FECHA_INCIAL AND ? <= FECHA_FINAL";
            Query query = getEntityManager().createNativeQuery(sql);
            query.setParameter(1, dateTime.toDate());
            query.setParameter(2, dateTime.toDate());
            BigDecimal result = (BigDecimal) query.getSingleResult();
            
            
            //System.out.println("::ANDRES6:: PORCENTAJE_SBSIS: " + result);
            
            
            return result;
        }
        catch (Exception e)
        {
            return new BigDecimal("0");
        }

   }

   
   public BigDecimal topeIbcArl(Nomina nomina, NominaDetalle nominaDetalle)
   {
        try
        {
            DateTime dateTime = new DateTime(nominaDetalle.getAno().intValue(), nominaDetalle.getMes().intValue(), 1, 0, 0);
            String sql = "SELECT VALOR_TOPE FROM COB_SBSIS WHERE TIP_PARAM_SBSIS = 403 AND CODIGO_SBSIS = 205"
                  + " AND (? BETWEEN FECHA_INCIAL AND FECHA_FINAL)";
            Query query = getEntityManager().createNativeQuery(sql);
            query.setParameter(1, dateTime.toDate());
            // query.setParameter(2, dateTime.toDate());
            BigDecimal result = (BigDecimal) query.getSingleResult();
            return result;
        }
        catch (Exception e)
        {
            return new BigDecimal("0");
        }

   }
   
   
   public BigDecimal tarifaFspSolidaridad(NominaDetalle nominaDetalle, BigDecimal ibcCalculadoPension)
   {
        try
        {
            
            DateTime dateTime = new DateTime(nominaDetalle.getAno().intValue(), nominaDetalle.getMes().intValue(), 1, 0, 0);
            
            /*
            if(nominaDetalle.getNumeroIdentificacionActual().equals("12345") && nominaDetalle.getAno().intValue() == 2017 && nominaDetalle.getMes().intValue() == 1)
            {
                System.out.println("::ANDRES40:: dateTime : " + dateTime);
                System.out.println("::ANDRES41:: ibcCalculadoPension : " + ibcCalculadoPension);
            }
            */


            String sql = "SELECT PORCENTAJE_SBSIS FROM COB_SBSIS WHERE TIP_PARAM_SBSIS = 402 AND CODIGO_SBSIS = 553"
                  + " AND (? BETWEEN FECHA_INCIAL AND FECHA_FINAL) AND (? BETWEEN RANGO_INICIAL AND RANGO_FINAL)";
            Query query = getEntityManager().createNativeQuery(sql);
            query.setParameter(1, dateTime.toDate());
            // query.setParameter(2, dateTime.toDate());
            query.setParameter(2, ibcCalculadoPension);
            // query.setParameter(4, ibcCalculadoPension);
            BigDecimal result = (BigDecimal) query.getSingleResult();
            return result;
        }
        catch (Exception e)
        {
            
            /*
            if(nominaDetalle.getNumeroIdentificacionActual().equals("12345") && nominaDetalle.getAno().intValue() == 2017 && nominaDetalle.getMes().intValue() == 1)
            {
                System.out.println("::ANDRES42:: retorno 0 exception : " + e.getMessage());
            }
            */
            
            return new BigDecimal("0");
        }
   }

   public BigDecimal tarifaFspSubsistencia(Nomina nomina, NominaDetalle nominaDetalle, BigDecimal ibcCalculadoPension)
   {
        try
        {    
            DateTime dateTime = new DateTime(nominaDetalle.getAno().intValue(), nominaDetalle.getMes().intValue(), 1, 0, 0);
            String sql = "SELECT PORCENTAJE_SBSIS FROM COB_SBSIS WHERE TIP_PARAM_SBSIS = 402 AND CODIGO_SBSIS = 204"
                  + " AND (? BETWEEN FECHA_INCIAL AND FECHA_FINAL) AND (? BETWEEN RANGO_INICIAL AND RANGO_FINAL)";
            Query query = getEntityManager().createNativeQuery(sql);
            query.setParameter(1, dateTime.toDate());
            // query.setParameter(2, dateTime.toDate());
            query.setParameter(2, ibcCalculadoPension);
            // query.setParameter(4, ibcCalculadoPension);
            BigDecimal result = (BigDecimal) query.getSingleResult();
            return result;
        }
        catch (Exception e)
        {
            return new BigDecimal("0");
        }
   }
   
   public BigDecimal obtenerSalarioDevengado(Nomina nomina, NominaDetalle nominaDetalle)
   {
        try
        {  
            String sql = "SELECT SUM(VALOR) AS VALOR FROM LIQ_CONCEPTO_CONTABLE WHERE IDNOMINADETALLE = ? "
                + "AND TIPO_PAGO_UGPP IN ('TP NO SALARIAL','TP SALARIAL') GROUP BY IDNOMINADETALLE";
            Query query = getEntityManager().createNativeQuery(sql);
            query.setParameter(1, nominaDetalle.getId());
            BigDecimal result = (BigDecimal) query.getSingleResult();
            return result;
        }
        catch (Exception e)
        {
            return new BigDecimal("0");
        }
   }

   
   
   public BigDecimal sumaValorLiqConceptoContableTotalRemunerado(Nomina nomina, NominaDetalle nominaDetalle)
   {

       
      try
      {
         // Modificación solicitada Feb.12.2016
    	  //String sqlAportante = "SELECT ID FROM LIQ_APORTANTE WHERE NUMERO_IDENTIFICACION = ? AND TIPO_APORTANTE NOT IN ('5','6','8') "
           //    + "AND NATURALEZA_JURIDICA = '2'";

    	  String sqlAportante = "SELECT ID FROM LIQ_APORTANTE WHERE NUMERO_IDENTIFICACION = ? AND NATURALEZA_JURIDICA IN('1','2','3')";
    	  
         Query queryApr = getEntityManager().createNativeQuery(sqlAportante, AportanteLIQ.class);
         queryApr.setParameter(1, nomina.getNit().toString());

         AportanteLIQ aportante = (AportanteLIQ) queryApr.getSingleResult();
         
         
         //System.out.println("::ANDRES11:: aportante: " + aportante.getId() + " MES: " + nominaDetalle.getMes());
         
         /*
         if(nominaDetalle.getIdcotizante().getNumeroIdentificacion().equals("1000393339"))
         {
             System.out.println("::ANDRES11:: aportante: " + aportante.getId() + " MES: " + nominaDetalle.getMes());
         }
         */

         if (aportante != null)
         {
            String sql = "SELECT SUM(VALOR) AS VALOR FROM LIQ_CONCEPTO_CONTABLE WHERE IDNOMINADETALLE = ? "
                  + "AND TIPO_PAGO_UGPP IN ('TP NO SALARIAL','TP SALARIAL','TP NO SALARIAL MARCADO SALARIAL POR LA UGPP','TP XX') "  + "GROUP BY IDNOMINADETALLE";

            Query query = getEntityManager().createNativeQuery(sql);
            query.setParameter(1, nominaDetalle.getId());

            BigDecimal result = (BigDecimal) query.getSingleResult();
            
            
            //System.out.println("::ANDRES12:: result: " + result + " MES: " + nominaDetalle.getMes() + " ID: " + nominaDetalle.getId()); 
            
            /*
            if(nominaDetalle.getIdcotizante().getNumeroIdentificacion().equals("1000393339"))
            {
                System.out.println("::ANDRES12:: result: " + result + " MES: " + nominaDetalle.getMes() + " ID: " + nominaDetalle.getId());
            }
            */
            

            return result;
         }
         else
         {
            return new BigDecimal("0");
         }

      }
      catch (Exception e)
      {
         //System.out.println("::sumaValorLiqConceptoContableTotalRemunerado:: Exception en PilaDepurada. Se devolvio 0." + e.getMessage()); 
         return new BigDecimal("0");
      }

   }

   
   
   
   
   
   
   
   
   
   
   
   
   
   
   public BigDecimal sumaValorLiqConceptoContableTotalDevengado(Nomina nomina, NominaDetalle nominaDetalle)
   {

       
      try
      {
         // Modificación solicitada Feb.12.2016
    	  //String sqlAportante = "SELECT ID FROM LIQ_APORTANTE WHERE NUMERO_IDENTIFICACION = ? AND TIPO_APORTANTE NOT IN ('5','6','8') "
           //    + "AND NATURALEZA_JURIDICA = '2'";

    	  String sqlAportante = "SELECT ID FROM LIQ_APORTANTE WHERE NUMERO_IDENTIFICACION = ? AND NATURALEZA_JURIDICA IN('1','2','3')";
    	  
         Query queryApr = getEntityManager().createNativeQuery(sqlAportante, AportanteLIQ.class);
         queryApr.setParameter(1, nomina.getNit().toString());

         AportanteLIQ aportante = (AportanteLIQ) queryApr.getSingleResult();
         
         //System.out.println("::ANDRES13:: aportante: " + aportante.getId() + " MES: " + nominaDetalle.getMes());
         
         /*
         if(nominaDetalle.getIdcotizante().getNumeroIdentificacion().equals("1000393339"))
         {
             System.out.println("::ANDRES11:: aportante: " + aportante.getId() + " MES: " + nominaDetalle.getMes());
         }
         */
         
         if (aportante != null)
         {
            String sql = "SELECT SUM(VALOR) AS VALOR FROM LIQ_CONCEPTO_CONTABLE WHERE IDNOMINADETALLE = ? "
                  + "AND TIPO_PAGO_UGPP IN ('TP NO SALARIAL','TP SALARIAL','TP NO SALARIAL MARCADO SALARIAL POR LA UGPP','TP VACACIONES','TP VACACIONES TERMINACION DE CONTRATO','TP LICENCIA REMUNERADA') "  
                  + "GROUP BY IDNOMINADETALLE";

            Query query = getEntityManager().createNativeQuery(sql);
            query.setParameter(1, nominaDetalle.getId());

            BigDecimal result = (BigDecimal) query.getSingleResult();
            
            //System.out.println("::ANDRES12:: result: " + result + " MES: " + nominaDetalle.getMes() + " ID: " + nominaDetalle.getId());
            
            
            /*
            if(nominaDetalle.getIdcotizante().getNumeroIdentificacion().equals("1000393339"))
            {
                System.out.println("::ANDRES12:: result: " + result + " MES: " + nominaDetalle.getMes() + " ID: " + nominaDetalle.getId());
            }
            */
            

            return result;
         }
         else
         {
            return new BigDecimal("0");
         }

      }
      catch (Exception e)
      {
         //System.out.println("::sumaValorLiqConceptoContableTotalDevengado:: Exception en PilaDepurada. Se devolvio 0." + e.getMessage());
         return new BigDecimal("0");
      }

   }
   
   
   
   
   
   
   
   
   
   
   
   
   
   
   
//   public PilaDetalle obtegerPilaDetalleNominaNominaDetalle(Nomina nomina, NominaDetalle nominaDetalle, CotizanteLIQ cotizanteLIQ)
//   {
//      String sql = "SELECT ID, CODIGO_EPS, DIAS_COT_SALUD, IBC_SALUD, TARIFA_SALUD, COT_OBLIGATORIA_SALUD, DIAS_COT_PENSION, IBC_PENSION, TARIFA_PENSION,"
//    		  + "COTIZACION_PAGADA_PENSION FROM LIQ_PILA_DETALLE "
//            + " WHERE NIT = ? AND NUMERO_IDENTIFICACION = ? AND PERIODO_RESTO = ?";
//
//      Query query = getEntityManager().createNativeQuery(sql, PilaDetalle.class);
//      query.setParameter(1, nomina.getNit());
//      query.setParameter(2, cotizanteLIQ.getNumeroIdentificacion());
//      query.setParameter(3, nominaDetalle.getAno().toString() + "" + nominaDetalle.getMes().toString());
//
//      try
//      {
//         PilaDetalle result = (PilaDetalle) query.getSingleResult();
//         return result;
//      }
//      catch (Exception e)
//      {
//         return null;
//      }
//
//   }

   public PilaDepurada obtegerPilaDepuradaNominaDetalle(Nomina nomina, NominaDetalle nominaDetalle)
   {
      PilaDepurada result = null; 
       
      // FIXME mas adelante colocar columna COTIZACION_PAGADA_PENSION
      String sql = "SELECT ID, CODIGO_EPS, DIAS_COT_SALUD, IBC_SALUD, TARIFA_SALUD, COT_OBLIGATORIA_SALUD, DIAS_COT_PENSION, IBC_PENSION, TARIFA_PENSION, "
    		  + "APORTE_COT_OBLIGATORIA_PENSION, VALOR_APORTES_CCF_IBC_TARIFA, APORTE_FSOLID_PENSIONAL_SOL, APORTE_FSOLID_PENSIONAL_SUB, CODIGO_ARP, TARIFA_CENTRO_TRABAJO,"
    		  + " DIAS_COT_RPROF, IBC_RPROF, COT_OBLIGATORIA_ARP, CODIGO_CCF, DIAS_COT_CCF, IBC_CCF, TARIFA_APORTES_CCF, TARIFA_APORTES_SENA,"
    		  +"  VALOR_APORTES_PARAFISCALES_SEN, TARIFA_APORTES_ICBF, VALOR_APORTES_PARAFISCALES_ICB, PLANILLA,TARIFA_MAXIMA, APOR_COR_OBLI_PEN_ALT_RIESGO, CODIGO_AFP"
    		  +"   FROM LIQ_PILA_DEPURADA "
            + " WHERE NIT = ? AND NUMERO_IDENTIFICACION = ? AND PERIODO_RESTO = ? ORDER BY ID DESC";

      Query query = getEntityManager().createNativeQuery(sql, PilaDepurada.class);
      query.setParameter(1, nomina.getNit());
      query.setParameter(2, nominaDetalle.getNumeroIdentificacionActual());

      String sMes = "";

      if (nominaDetalle.getMes().intValue() <= 9)
         sMes = "0" + nominaDetalle.getMes().intValue();
      else
         sMes = nominaDetalle.getMes().toString();

      query.setParameter(3, nominaDetalle.getAno().toString() + "" + sMes);

      try
      {
         result = (PilaDepurada) query.getSingleResult();
         return result;
      }
      catch (NonUniqueResultException nure)
      {
          //System.out.println("::obtegerPilaDepuradaNominaDetalle:: NonUniqueResultException, se devolvio el primer registro. " + nure.getMessage()); 
          return (PilaDepurada)query.getResultList().get(0);
      }
      catch (Exception e)
      {
         //System.out.println("::obtegerPilaDepuradaNominaDetalle:: Exception en PilaDepurada. Se devolvio null." + e.getMessage());  
         return null;
      }

   }
   
   

   public PilaDepurada obtegerPilaDepuradaNominaDetalleCotizanteRealizoAportes(Nomina nomina, NominaDetalle nominaDetalle)
   {
      // FIXME mas adelante colocar columna COTIZACION_PAGADA_PENSION
      String sql = "SELECT ID, CODIGO_EPS, DIAS_COT_SALUD, IBC_SALUD, TARIFA_SALUD, COT_OBLIGATORIA_SALUD, DIAS_COT_PENSION, IBC_PENSION, TARIFA_PENSION, "
    		  + "APORTE_COT_OBLIGATORIA_PENSION, VALOR_APORTES_CCF_IBC_TARIFA, APORTE_FSOLID_PENSIONAL_SOL, APORTE_FSOLID_PENSIONAL_SUB, CODIGO_ARP, TARIFA_CENTRO_TRABAJO,"
    		  + " DIAS_COT_RPROF, IBC_RPROF, COT_OBLIGATORIA_ARP, CODIGO_CCF, DIAS_COT_CCF, IBC_CCF, TARIFA_APORTES_CCF, TARIFA_APORTES_SENA,"
    		  +"  VALOR_APORTES_PARAFISCALES_SEN, TARIFA_APORTES_ICBF, VALOR_APORTES_PARAFISCALES_ICB, PLANILLA,TARIFA_MAXIMA, CODIGO_AFP"
    		  +"   FROM LIQ_PILA_DEPURADA "
            + " WHERE NIT = ? AND NUMERO_IDENTIFICACION = ? AND PERIODO_RESTO = ? ORDER BY ID DESC";

      Query query = getEntityManager().createNativeQuery(sql, PilaDepurada.class);
      query.setParameter(1, nomina.getNit());
      query.setParameter(2, nominaDetalle.getNumeroIdentificacionRealizoAportes());

      String sMes = "";

      if (nominaDetalle.getMes().intValue() <= 9)
         sMes = "0" + nominaDetalle.getMes().intValue();
      else
         sMes = nominaDetalle.getMes().toString();

      query.setParameter(3, nominaDetalle.getAno().toString() + "" + sMes);

      
      //System.out.println("::ANDRES04:: getNit: " + nomina.getNit()); 
      //System.out.println("::ANDRES04:: getNumeroIdentificacionRealizoAportes: " + cotizanteLIQ.getNumeroIdentificacionRealizoAportes());   
      //System.out.println("::ANDRES04:: getAno: " + nominaDetalle.getAno().toString() + "" + sMes);
      
      try
      {
         PilaDepurada result = (PilaDepurada) query.getSingleResult();
         return result;
      }
      catch (NonUniqueResultException nure)
      {
          //System.out.println("::obtegerPilaDepuradaNominaDetalleCotizanteRealizoAportes:: NonUniqueResultException, se devolvio el primer registro. " + nure.getMessage()); 
          return (PilaDepurada)query.getResultList().get(0);
      }
      catch (Exception e)
      {
         //System.out.println("::obtegerPilaDepuradaNominaDetalleCotizanteRealizoAportes:: Exception en PilaDepurada. Se devolvio null." + e.getMessage());  
         return null;
      }

   }
   
   
   
   
   
   
   
   
   
   
   
   

   public float obtenerTarifaCentroTrabajoPilaDepuradaNominaDetalle(Nomina nomina, NominaDetalle nominaDetalle)
   {
      
      //EN TEORIA SIEMPRE SACA SOLO UN REGISTRO, SE COLOCA CON LA FUNCION MAX PARA
      //QUE NO MUESTRE LA EXCEPCION EN ECLIPSE LINK DE MULTIPLES REGISTROS EN UN SINGLERESULT
      String sql = "SELECT MAX(TARIFA_CENTRO_TRABAJO) AS VALOR FROM LIQ_PILA_DEPURADA WHERE NIT = ? AND PERIODO_RESTO = ? AND NUMERO_IDENTIFICACION = ?";
      //String sql = "SELECT TARIFA_CENTRO_TRABAJO AS VALOR FROM LIQ_PILA_DEPURADA WHERE NIT = ? AND PERIODO_RESTO = ? AND NUMERO_IDENTIFICACION = ?";

      Query query = getEntityManager().createNativeQuery(sql);
      query.setParameter(1, nomina.getNit());

      String sMes = "";

      if (nominaDetalle.getMes().intValue() <= 9)
         sMes = "0" + nominaDetalle.getMes().intValue();
      else
         sMes = nominaDetalle.getMes().toString();

      query.setParameter(2, nominaDetalle.getAno().toString() + "" + sMes);
      query.setParameter(3, nominaDetalle.getNumeroIdentificacionActual());
      

      try
      {
         BigDecimal result = (BigDecimal) query.getSingleResult();
         return result.floatValue();
         
      }
      catch (Exception e)
      {
         //System.out.println("::ERROR:: exception: " + e.getMessage());
         return 0f;
      }

   }
   
   
   
   
    public float obtenerMaximaTarifaCentroTrabajoPilaDepuradaNominaDetalle(Nomina nomina, NominaDetalle nominaDetalle)
    {
        
        //String sql = "SELECT MAX(TARIFA_CENTRO_TRABAJO) AS VALOR FROM LIQ_PILA_DEPURADA WHERE NIT = ? AND NUMERO_IDENTIFICACION = ?";
        String sql = "SELECT MAX(TARIFA_CENTRO_TRABAJO) AS VALOR FROM LIQ_PILA_DEPURADA WHERE NIT = ? AND NUMERO_IDENTIFICACION = ? AND ANO = ?";
        
        
        Query query = getEntityManager().createNativeQuery(sql);
        query.setParameter(1, nomina.getNit());
        query.setParameter(2, nominaDetalle.getNumeroIdentificacionActual());
        query.setParameter(3, nominaDetalle.getAno().toString());
        
        try
        {
          BigDecimal result = (BigDecimal) query.getSingleResult();
          return result.floatValue();
        }
        catch (Exception e)
        {
           //System.out.println("::ERROR:: obtenerMaximaTarifaCentroTrabajoPilaDepuradaNominaDetalle: " + e.getMessage());
           return 0f;
        }

   }
   
    
    
    
        public float obtenerMaximaTarifaAportante(Nomina nomina, String anio)
    {
        //String sql = "SELECT MAX(TARIFA_CENTRO_TRABAJO) AS VALOR FROM LIQ_PILA_DEPURADA WHERE NIT = ? ";
        String sql = "SELECT MAX(TARIFA_CENTRO_TRABAJO) AS VALOR FROM LIQ_PILA_DEPURADA WHERE NIT = ? AND ANO = ?";
    
       
        Query query = getEntityManager().createNativeQuery(sql);
        query.setParameter(1, nomina.getNit());
        query.setParameter(2, anio);

        try
        {
          BigDecimal result = (BigDecimal) query.getSingleResult();
          return result.floatValue();
        }
        catch (Exception e)
        {
           //System.out.println("::ERROR:: obtenerMaximaTarifaAportante: " + e.getMessage());
           return 0f;
        }

   }
   
    
   
   
        
         
    
    
    public int verificarNitPilaDepurada(String nit)
    {
        //String sql = "SELECT MAX(TARIFA_CENTRO_TRABAJO) AS VALOR FROM LIQ_PILA_DEPURADA WHERE NIT = ? ";
        String sql = "SELECT COUNT(NIT) FROM LIQ_PILA_DEPURADA WHERE NIT = ?";
    
       
        Query query = getEntityManager().createNativeQuery(sql);
        query.setParameter(1, nit);

        try
        {
          BigDecimal result = (BigDecimal) query.getSingleResult();
          return result.intValue();
        }
        catch (Exception e)
        {
           //System.out.println("::Exception:: verificarNitPilaDepurada nit: " + nit + " Se devolvio 0. EX: " + e.getMessage());  
           return 0;
        }
   }
    
        
        
   
   
   
   public PilaDepurada obtegerPilaDepuradaNominaDetalleCache(Map<String, Object> mapPilaDepurada, Nomina nomina,
         NominaDetalle nominaDetalle)
   {

      String sMes = "";

      if (nominaDetalle.getMes().intValue() <= 9)
         sMes = "0" + nominaDetalle.getMes().intValue();
      else
         sMes = nominaDetalle.getMes().toString();

      String pilaKey = nominaDetalle.getNumeroIdentificacionActual() + "#" + nominaDetalle.getAno().toString() + "" + sMes;

      if (mapPilaDepurada.containsKey(pilaKey))
      {
          //System.out.println("::ANDRES5:: piladepurada de CACHE pilaKey: " + pilaKey);
          return (PilaDepurada) mapPilaDepurada.get(pilaKey);
      }
      else
      {
         PilaDepurada pilaDepurada = obtegerPilaDepuradaNominaDetalle(nomina, nominaDetalle);
         mapPilaDepurada.put(pilaKey, pilaDepurada);
         
         //System.out.println("::ANDRES5:: piladepurada de BD nit-cotizante-mes-ano: " + nomina.getNit() + "-" +  cotizanteLIQ.getNumeroIdentificacion() +"-"+ nominaDetalle.getMes()+"-"+nominaDetalle.getAno());
         return pilaDepurada;
      }

   }

   public CobFlex obtegerCobFlexByEstado(String estado)
   {
      String sql = "SELECT * FROM COB_FLEX ESTADO = ?";

      Query query = getEntityManager().createNativeQuery(sql, CobFlex.class);
      query.setParameter(1, estado);

      try
      {
         CobFlex result = (CobFlex) query.getSingleResult();
         return result;
      }
      catch (Exception e)
      {
         //System.out.println("::Exception:: obtegerCobFlexByEstado estado: " + estado + " EX: " + e.getMessage());
         return null;
      }

   }

   public CobFlex obtenerCobFlexByFecha(NominaDetalle obj)
   {
      // obtener año y mes anterior para hacer filtro
      DateTime dateTime = new DateTime(obj.getAno().intValue(), obj.getMes().intValue(), 1, 0, 0);

      /*
      if(obj.getNumeroIdentificacionActual().equals("12345") && obj.getAno().intValue() == 2016 && obj.getMes().intValue() == 8)
      {
          System.out.println("::ANDRES02:: dateTime: " + dateTime.toDate().toString());        
      } 
      */
      
      String sql = "SELECT * FROM COB_FLEX WHERE ? >= FECHA_INCIAL AND ? <= FECHA_FINAL";

      Query query = getEntityManager().createNativeQuery(sql, CobFlex.class);
      query.setParameter(1, dateTime.toDate());
      query.setParameter(2, dateTime.toDate());

      try
      {
        
          CobFlex result = (CobFlex) query.getSingleResult();
         
        /* 
        if(obj.getNumeroIdentificacionActual().equals("12345") && obj.getAno().intValue() == 2016 && obj.getMes().intValue() == 8)
        {
            System.out.println("::ANDRES03:: result: " + result);  
        }
        */

        return result;
      }
      catch (Exception e)
      {
        
        /*
        if(obj.getNumeroIdentificacionActual().equals("12345") && obj.getAno().intValue() == 2016 && obj.getMes().intValue() == 8)
        {
          System.out.println("::Exception:: exception devuelve null: " + e.getMessage()); 
        }
        */
        
        return null;
      }

   }



}
