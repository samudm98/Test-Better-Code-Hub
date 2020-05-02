package co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.OpLiquidarSolTipo;
import co.gov.ugpp.parafiscales.servicios.liquidador.entity.HojaCalculoLiquidacion;
import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.entity.InInstPrograma;
import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.entity.InInstProgramaRegla;
import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.entity.InRegla;
import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.jpa.JPAEntityDao;
import java.io.Serializable;
import javax.ejb.Stateless;
import javax.ejb.TransactionManagement;
import javax.persistence.PersistenceContext;
import org.slf4j.LoggerFactory;

@Stateless
@TransactionManagement
public class AdministrarPersistencia implements Serializable {

    @PersistenceContext
    private EntityManager entityManager;

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(AdministrarPersistencia.class);

    public AdministrarPersistencia() {

    }

    public AdministrarPersistencia(EntityManager paramEntityManager) {
        this.entityManager = paramEntityManager;
    }

    /**
     *
     * @param idPrograma
     * @param msjOpLiquidarSol
     * @return 
     */
    public InInstPrograma registrarInicioEjecucionInstanciaPrograma(String idPrograma, OpLiquidarSolTipo msjOpLiquidarSol) {
        JPAEntityDao jpaEntityDao = new JPAEntityDao(getEntityManager());

        InInstPrograma obj = new InInstPrograma();
        obj.setIdInstanciaPrograma(UUID.randomUUID().toString());
        obj.setFecCreacionRegistro(new Date());
        obj.setIdPrograma(idPrograma);
        obj.setIdUsuarioCreacion(msjOpLiquidarSol.getContextoTransaccional().getIdUsuario());
        //System.out.println("== YESID === INICIA EL PROGRAMA "+obj.getFecCreacionRegistro());
        //  this.entityManager.getTransaction().begin();

        jpaEntityDao.save(obj, true);

        //  this.entityManager.getTransaction().commit();
        return obj;
    }

    public void registrarFinEjecucionInstanciaPrograma(InInstPrograma inInstPrograma, String idPrograma,
            OpLiquidarSolTipo msjOpLiquidarSol) {
        JPAEntityDao jpaEntityDao = new JPAEntityDao(getEntityManager());

        InInstPrograma obj = jpaEntityDao.findById(InInstPrograma.class, inInstPrograma.getIdInstanciaPrograma());

        //  this.entityManager.getTransaction().begin();
        obj.setFecModificacionRegistro(new Date());
        //System.out.println("== YESID === FINALIZA EL PROGRAMA "+obj.getFecModificacionRegistro());

        jpaEntityDao.save(obj, true);

        //  this.entityManager.getTransaction().commit();
    }

    public InInstProgramaRegla registrarInicioEjecucionRegla(Integer numOrder, InInstPrograma inInstPrograma, String idRegla,
            String idPrograma, OpLiquidarSolTipo msjOpLiquidarSol) {
        JPAEntityDao jpaEntityDao = new JPAEntityDao(getEntityManager());

        InInstProgramaRegla obj = new InInstProgramaRegla();
        obj.setId(UUID.randomUUID().toString());
        obj.setFecCreacionRegistro(new Date());
        obj.setIdInstanciaPrograma(inInstPrograma.getIdInstanciaPrograma());
        obj.setIdPrograma(idPrograma);
        obj.setIdRegla(idRegla);
        obj.setIdUsuarioCreacion(msjOpLiquidarSol.getContextoTransaccional().getIdUsuario());
        obj.setNumOrder(numOrder);

        // this.entityManager.getTransaction().begin();
        jpaEntityDao.save(obj, true);

        //  this.entityManager.getTransaction().commit();
        return obj;
    }

    public void registrarFinEjecucionRegla(InInstProgramaRegla inInstProgramaRegla, InInstPrograma inInstPrograma,
            String idRegla, String idPrograma, OpLiquidarSolTipo msjOpLiquidarSol) {
        JPAEntityDao jpaEntityDao = new JPAEntityDao(getEntityManager());

        InInstProgramaRegla instObj = jpaEntityDao.findById(InInstProgramaRegla.class, inInstProgramaRegla.getId());

        // this.entityManager.getTransaction().begin();
        instObj.setFecModificacionRegistro(new Date());

        jpaEntityDao.save(instObj, true);

        //  this.entityManager.getTransaction().commit();
    }

    public EntityManager getEntityManager() {
        return entityManager;
    }

    public void setEntityManager(EntityManager entityManager) {
        this.entityManager = entityManager;
    }

    public HojaCalculoLiquidacion registrarHojaCalculoLiquidacion(OpLiquidarSolTipo msjOpLiquidarSol) {

        String sqlId = "SELECT seqhojacalculoliquidador.NEXTVAL FROM DUAL";
        Query queryId = getEntityManager().createNativeQuery(sqlId);
        Object idObj = queryId.getSingleResult();

        // this.entityManager.getTransaction().begin();
        HojaCalculoLiquidacion obj = new HojaCalculoLiquidacion();
        obj.setId((BigDecimal) idObj);
        obj.setIdexpediente(msjOpLiquidarSol.getExpediente().getIdNumExpediente());
        obj.setFechaCreacion(Calendar.getInstance());

        String sql = "INSERT INTO LIQ_HOJA_CALCULO_LIQUIDACION (ID,IDEXPEDIENTE,FECHACREACION) VALUES (?,?,?) ";

        Query query = getEntityManager().createNativeQuery(sql);
        query.setParameter(1, idObj);
        query.setParameter(2, msjOpLiquidarSol.getExpediente().getIdNumExpediente());
        query.setParameter(3, Calendar.getInstance());

        query.executeUpdate();

        // this.entityManager.getTransaction().commit();
        return obj;

    }

    public void guardarResultadoRegla(List<DatosEjecucionRegla> listDatEjeRegla, InRegla regla,
            OpLiquidarSolTipo msjOpLiquidarSol, HojaCalculoLiquidacion hojaCalculoLiquidacion, Boolean primeraRegla,
            Map<String, Object> infoNegocio) {
        //LOG.info("Inside by guardarResultadoRegla()");
        // this.entityManager.getTransaction().begin();

        BigDecimal idHojaDetal = null;

        for (int i = 0; i < listDatEjeRegla.size(); i++) {
            DatosEjecucionRegla obj = listDatEjeRegla.get(i);

            if (primeraRegla) {
                String sqlId = "SELECT seqhojacalculoliquidadordetal.NEXTVAL FROM DUAL";
                Query queryId = getEntityManager().createNativeQuery(sqlId);
                idHojaDetal = (BigDecimal) queryId.getSingleResult();

                String sql = "INSERT INTO HOJA_CALCULO_LIQUIDACION_DETAL (ID,IDHOJACALCULOLIQUIDACION,IDNOMINADETALLE,IDCONCILIACIONCONTABLEDETALLE,APORTA_ID,APORTA_APORTE_ESAP_Y_MEN,APORTA_CLASE,APORTA_EXCEPCION_LEY_1233_2008,"
                        + "APORTA_PRIMER_NOMBRE, APORTA_NUMERO_IDENTIFICACION,COTIZ_ID,COTIZ_ACTIVIDAD_ALTO_RIESGO_PE,COTIZ_COLOMBIANO_EN_EL_EXT, COTIZ_EXTRANJERO_NO_COTIZAR,COTIZ_SUBTIPO_COTIZANTE,COTIZ_TIPO_COTIZANTE,"
                        + "COTIZ_NOMBRE, COTIZ_NUMERO_IDENTIFICACION, COTIZD_ANO, COTIZD_MES, "
                        + regla.getCodigo()
                        + ",FEC_CREACION) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";

                Query query = getEntityManager().createNativeQuery(sql);
                query.setParameter(1, idHojaDetal);
                query.setParameter(2, hojaCalculoLiquidacion.getId());
                query.setParameter(3, obj.getNominaDetalle().getId());
                query.setParameter(4, 0);// FIXME dato de ejemplo
                query.setParameter(5, obj.getNominaDetalle().getIdaportante().getId());
                query.setParameter(6, obj.getNominaDetalle().getIdaportante().getAportaEsapYMen());
                query.setParameter(7, obj.getNominaDetalle().getIdaportante().getClase());
                query.setParameter(8, obj.getNominaDetalle().getIdaportante().getExcepcionLey12332008());
                query.setParameter(9, obj.getNominaDetalle().getIdaportante().getPrimerNombre());
                query.setParameter(10, obj.getNominaDetalle().getIdaportante().getNumeroIdentificacion());
                
                // Datos del cotizante
                //query.setParameter(11, obj.getNominaDetalle().getIdcotizante().getId());
                
                query.setParameter(12, obj.getNominaDetalle().getActividad_alto_riesgo_pension());
                query.setParameter(13, obj.getNominaDetalle().getColombiano_en_el_exterior());
                query.setParameter(14, obj.getNominaDetalle().getExtranjero_no_obligado_a_cotizar_pension());
                query.setParameter(15, obj.getNominaDetalle().getSubTipoCotizante());
                query.setParameter(16, obj.getNominaDetalle().getTipoCotizante());
                query.setParameter(17, obj.getNominaDetalle().getNombre());
                query.setParameter(18, obj.getNominaDetalle().getNumeroIdentificacionActual());
                query.setParameter(19, obj.getNominaDetalle().getAno());
                query.setParameter(20, obj.getNominaDetalle().getMes());

                String key = obj.getNominaDetalle().getNumeroIdentificacionActual() + "#" + regla.getCodigo() + "#"
                        + obj.getNominaDetalle().getAno().toString() + obj.getNominaDetalle().getMes().toString();
                Object result = infoNegocio.get(key);
                query.setParameter(21, result);

                query.setParameter(22, Calendar.getInstance());

                query.executeUpdate();
            } else {
                String sql = "UPDATE HOJA_CALCULO_LIQUIDACION_DETAL SET " + regla.getCodigo()
                        + " = ?, FEC_MODIFICACION = ? WHERE IDHOJACALCULOLIQUIDACION = ? AND IDNOMINADETALLE = ?";

                String key = obj.getNominaDetalle().getNumeroIdentificacionActual() + "#" + regla.getCodigo() + "#"
                        + obj.getNominaDetalle().getAno().toString() + obj.getNominaDetalle().getMes().toString();
                Object result = infoNegocio.get(key);

                /* AL PARECER AQUI NUNCA ENTRA Y NO SE PORQUE, NO SALIERON LOS MENSAJES
               System.out.println("::ANDRES8:: getCodigo " + regla.getCodigo());
               if(regla.getCodigo().equals("COD_ADM_PENSION"))
               {
                   System.out.println("::ANDRES_COD_ADM_PENSION:: key " + result);
                   System.out.println("::ANDRES_COD_ADM_PENSION:: result " + result);
                   System.out.println("::ANDRES_COD_ADM_PENSION:: hojaCalculoLiquidacion " + hojaCalculoLiquidacion.getId());
                   System.out.println("::ANDRES_COD_ADM_PENSION:: getNominaDetalle " + obj.getNominaDetalle().getId());
               }
               
               if(regla.getCodigo().equals("NOM_ADM_PENSION"))
               {
                   System.out.println("::ANDRES_NOM_ADM_PENSION:: key " + result);
                   System.out.println("::ANDRES_NOM_ADM_PENSION:: result " + result);
                   System.out.println("::ANDRES_NOM_ADM_PENSION:: hojaCalculoLiquidacion " + hojaCalculoLiquidacion.getId());
                   System.out.println("::ANDRES_NOM_ADM_PENSION:: getNominaDetalle " + obj.getNominaDetalle().getId());
               }
                 */
                Query query = getEntityManager().createNativeQuery(sql);
                query.setParameter(1, result);
                query.setParameter(2, Calendar.getInstance());
                query.setParameter(3, hojaCalculoLiquidacion.getId());
                query.setParameter(4, obj.getNominaDetalle().getId());

                query.executeUpdate();
            }

        }

        //   this.entityManager.getTransaction().commit();
    }

    public void guardarResultadoReglaNominaDetalle(DatosEjecucionRegla obj, List<InRegla> reglaList,
            OpLiquidarSolTipo msjOpLiquidarSol, HojaCalculoLiquidacion hojaCalculoLiquidacion, Map<String, Object> infoNegocio) {
        // LOG.info("Inside by guardarResultadoReglaNominaDetalle()");
        //  this.entityManager.getTransaction().begin();
        final String parameters_To_Set = "Parameters_To_Set";
        final String values_To_Set = "Values_To_Set";
        try {
            BigDecimal idHojaDetal = null;
            InRegla regla = null;
            String key = null;
            String sqlId = "SELECT seqhojacalculoliquidadordetal.NEXTVAL FROM DUAL";
            Query queryId = getEntityManager().createNativeQuery(sqlId);
            idHojaDetal = (BigDecimal) queryId.getSingleResult();

            String sql = "INSERT INTO HOJA_CALCULO_LIQUIDACION_DETAL (ID,IDHOJACALCULOLIQUIDACION,IDNOMINADETALLE,IDCONCILIACIONCONTABLEDETALLE,APORTA_ID,APORTA_APORTE_ESAP_Y_MEN,APORTA_CLASE,APORTA_EXCEPCION_LEY_1233_2008,"
                    + "APORTA_PRIMER_NOMBRE, APORTA_NUMERO_IDENTIFICACION,COTIZ_ID,COTIZ_ACTIVIDAD_ALTO_RIESGO_PE,COTIZ_COLOMBIANO_EN_EL_EXT, COTIZ_EXTRANJERO_NO_COTIZAR,COTIZ_SUBTIPO_COTIZANTE,COTIZ_TIPO_COTIZANTE,"
                    + "COTIZ_NOMBRE, COTIZ_NUMERO_IDENTIFICACION, COTIZD_ANO, COTIZD_MES, FEC_CREACION, " 
                    + parameters_To_Set + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " + values_To_Set + ") ";

            // Se cargan los nombres de campos a actualizar
            for (int i = 0; i < reglaList.size(); i++) 
            {
                regla = reglaList.get(i);
                key = obj.getNominaDetalle().getNumeroIdentificacionActual() + "#" + regla.getCodigo() + "#"
                        + obj.getNominaDetalle().getAno().toString() + obj.getNominaDetalle().getMes().toString();
                
                //System.out.println("::ANDRES8:: key: " + key);
                
                if(infoNegocio.get(key) != null) 
                {
                    sql = sql.replace(parameters_To_Set, (regla.getCodigo() + ", " + parameters_To_Set));
                    sql = sql.replace(values_To_Set, ("?, " + values_To_Set));
                }
            }
            
            //System.out.println("::ANDRES6:: size listRegla: " + sql);
            
            sql = sql.replace(", " + parameters_To_Set, "") ;
            
            //System.out.println("::ANDRES7:: parameters_To_Set: " + parameters_To_Set);
            
            sql = sql.replace(", " + values_To_Set, "") ;

            //System.out.println("::ANDRES8:: values_To_Set" + values_To_Set);
            

            //System.out.println("::ANDRES9:: SQL" + sql);

            
            
            Query query = getEntityManager().createNativeQuery(sql);

            query.setParameter(1, idHojaDetal);
            query.setParameter(2, hojaCalculoLiquidacion.getId());
            query.setParameter(3, obj.getNominaDetalle().getId());
            query.setParameter(4, 0);// FIXME dato de ejemplo
            query.setParameter(5, obj.getNominaDetalle().getIdaportante().getId());
            query.setParameter(6, obj.getNominaDetalle().getIdaportante().getAportaEsapYMen());
            query.setParameter(7, obj.getNominaDetalle().getIdaportante().getClase());
            query.setParameter(8, obj.getNominaDetalle().getIdaportante().getExcepcionLey12332008());
            query.setParameter(9, obj.getNominaDetalle().getIdaportante().getPrimerNombre());
            query.setParameter(10, obj.getNominaDetalle().getIdaportante().getNumeroIdentificacion()); // Datos del cotizante
            query.setParameter(11, null);
            query.setParameter(12, obj.getNominaDetalle().getActividad_alto_riesgo_pension());
            query.setParameter(13, obj.getNominaDetalle().getColombiano_en_el_exterior());
            query.setParameter(14, obj.getNominaDetalle().getExtranjero_no_obligado_a_cotizar_pension());
            query.setParameter(15, obj.getNominaDetalle().getSubTipoCotizante());
            query.setParameter(16, obj.getNominaDetalle().getTipoCotizante());
            query.setParameter(17, obj.getNominaDetalle().getNombreCotizante());
            query.setParameter(18, obj.getNominaDetalle().getNumeroIdentificacionActual());
            query.setParameter(19, obj.getNominaDetalle().getAno());
            query.setParameter(20, obj.getNominaDetalle().getMes());
            query.setParameter(21, Calendar.getInstance());

            // Se cargan los valores de parametros almacenados en el Map
            int cont = 22;
            
            for (int i = 0; i < reglaList.size(); i++) {
                regla = reglaList.get(i);
                key = obj.getNominaDetalle().getNumeroIdentificacionActual() + "#" + regla.getCodigo() + "#"
                        + obj.getNominaDetalle().getAno().toString() + obj.getNominaDetalle().getMes().toString();
                
                //System.out.println("::ANDRES10:: key: " + key);
                
                if(infoNegocio.get(key) != null) {
                    Object value = infoNegocio.get(key);
                    //System.out.println("::ANDRES11:: contador: " + cont + " value: " + value);
                    if(value.getClass().equals(String.class)) {
                        value = value.toString().trim();
                    }
                    query.setParameter((cont++), value);
                }
            }

            //LOG.info("::ANDRES12:: Cantidad de columnas a realizar update: " + reglaList.size());
            
            query.executeUpdate();

            //  this.entityManager.getTransaction().commit();
            // LOG.info("Outside by guardarResultadoReglaNominaDetalle()");
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    public void MedirTiempos(HojaCalculoLiquidacion hojaCalculoLiquidacion, DatosEjecucionRegla obj, java.sql.Timestamp tiempoini, java.sql.Timestamp tiempofin, String Regla) {

        /*
       
       DatosEjecucionRegla obj, List<InRegla> reglaList,
         OpLiquidarSolTipo msjOpLiquidarSol, HojaCalculoLiquidacion hojaCalculoLiquidacion, Map<String, Object> infoNegocio)
         */
        String sql = "INSERT INTO HOJA_CALCULO_LIQUIDACION_TMP  (IDHOJACALCULOLIQUIDACION,IDNOMINADETALLE, FEC_INICIO,FEC_FIN,REGLA) "
                + " VALUES (?,?,?,?,?)";
        Query query = getEntityManager().createNativeQuery(sql);

        query.setParameter(1, hojaCalculoLiquidacion.getId());
        query.setParameter(2, obj.getNominaDetalle().getId());
        query.setParameter(3, tiempoini);
        query.setParameter(4, tiempofin);
        query.setParameter(5, Regla);
        query.executeUpdate();

    }
}
