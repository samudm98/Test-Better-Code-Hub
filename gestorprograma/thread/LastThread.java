package co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.thread;

import co.gov.ugpp.parafiscales.servicios.liquidador.entity.HojaCalculoLiquidacion;
import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.AbstractModoEjecucion;
import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.AdministrarPersistencia;
import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.entity.InInstPrograma;
import static co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.thread.ParentThread.HOJA_CALCULO_LIQUIDACION;
import co.gov.ugpp.parafiscales.servicios.liquidador.transversales.IntegracionBPM;
import co.gov.ugpp.parafiscales.servicios.liquidador.util.SQLConection;
import java.sql.SQLException;
import javax.persistence.EntityManager;
import org.slf4j.LoggerFactory;

/**
 * @since 19/Jul/2017
 * @author UT_TECNOLOGI
 */
public class LastThread extends ParentThread {

    private static final long serialVersionUID = 1L;
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(LastThread.class);

    InInstPrograma inInstPrograma;
    String idPrograma;
    EntityManager em;

    /**
     * Constructor de la clase LastThread Este debe ser siempre el ultimo hilo
     * en ejecutarse
     *
     * @param modoEjecucionService
     * @param inInstPrograma
     * @param idPrograma
     * @param administrarPersistencia
     * @param em
     */
    public LastThread(AbstractModoEjecucion modoEjecucionService,
            InInstPrograma inInstPrograma, String idPrograma,
            AdministrarPersistencia administrarPersistencia, EntityManager em) {
        this.administrarPersistencia = administrarPersistencia;
        this.em = em;
        this.modoEjecucionService = modoEjecucionService;
        this.inInstPrograma = inInstPrograma;
        this.idPrograma = idPrograma;
    }

    /**
     * Metodo principal de ejecucion del hilo
     */
    @Override
    public void run() {
        try {
            setNewName();
            iniciarTarea();
            //LOG.info("opLiquidar Ultimo Hilo: " + Thread.currentThread().getName() + " Start.");
            modoEjecucionService.cerrarLstAdministradoraPila();
            administrarPersistencia.setEntityManager(em);
            liquidadorProceso(HOJA_CALCULO_LIQUIDACION);
            administrarPersistencia.registrarFinEjecucionInstanciaPrograma(inInstPrograma, idPrograma, MSJ_OP_LIQUIDAR_SOL);

            IntegracionBPM.servicioIntegracionRecibirLiquidacion(HOJA_CALCULO_LIQUIDACION.getId().toString(), "OK", "OK", MSJ_OP_LIQUIDAR_SOL.getContextoTransaccional());
                      
            
        
        } catch (Exception ex) {
            ex.printStackTrace();
            LOG.error("ERROR Exception - " + Thread.currentThread().getName(), ex);
        } finally {
            LOG.info("opLiquidar " + Thread.currentThread().getName() + " - Se terminaron de ejecutar todos los hilos.");
            
            //Double promedio = Double.valueOf(sumaTiempos) / Double.valueOf(TOTAL_THREAD) / 1000D;
            //DecimalFormat df = new DecimalFormat("#0,0##");
            //LOG.info("Suma de ejecucion: " + df.format(Double.valueOf(sumaTiempos) / 1000D)  + " Seg.");
            //LOG.info("Tiempo promedio de ejecucion: " + df.format(promedio)  + " Seg.");
            
            administrarPersistencia = null;
            modoEjecucionService = null;
            inInstPrograma = null;
            idPrograma = null;
            liberarHilo();
        }
    }

    /**
     * Metodo encargado de liquidar procesos de doble linea
     *
     * @param hojaCalculoLiquidacion
     */
    void liquidadorProceso(HojaCalculoLiquidacion hojaCalculoLiquidacion) {
        SQLConection sqlConection = null;

        try {
            
            sqlConection = SQLConection.getInstance();
            // PROCEDIMIENTO ALMACENADO DE HENRY COMPARACION DE LIQUIDACIONES DE LA
            // ETAPA DE REQUERIMIENTO Y ETAPA DE LIQUIDACION
            sqlConection.liquidadorComparacionVariacionIBC(hojaCalculoLiquidacion.getId().toString());
            sqlConection.liquidadorDobleLineaProceso2B(hojaCalculoLiquidacion.getId().toString());
            
        } catch (NullPointerException ne) {
            LOG.error("ERROR opLiquidar NullPointerException", ne);
        } catch (Exception e) {
            LOG.error("Exception opLiquidar", e);
        } finally {
            try {
                if (sqlConection != null) {
                    sqlConection.closeConnection();
                }
            } catch (SQLException ex) {
                LOG.error("SQLException opLiquidar", ex);
            }

        }
    }

}
