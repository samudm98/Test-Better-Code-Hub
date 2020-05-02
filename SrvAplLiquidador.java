package co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion;

import co.gov.ugpp.parafiscales.servicios.liquidador.transversales.AbstractSrvApl;
import co.gov.ugpp.parafiscales.servicios.liquidador.transversales.AppException;
import co.gov.ugpp.parafiscales.servicios.liquidador.contextorespuestatipo.v1.ContextoRespuestaTipo;
import co.gov.ugpp.parafiscales.servicios.liquidador.contextotransaccionaltipo.v1.ContextoTransaccionalTipo;
import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.AdministrarPersistencia;
import co.gov.ugpp.parafiscales.servicios.liquidador.util.ErrorUtil;
import javax.ejb.EJB;
import javax.jws.WebService;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import org.slf4j.LoggerFactory;

import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.GestorProgramaService;
import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.thread.ParentThread;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * @author franzjr
 */
@WebService(serviceName = "SrvAplLiquidador",
        portName = "portSrvAplLiquidador",
        endpointInterface = "co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.PortSrvAplLiquidador")

public class SrvAplLiquidador extends AbstractSrvApl {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SrvAplLiquidador.class);

    @EJB
    private LiquidadorFacade liquidadorFacade;

    @PersistenceContext
    private EntityManager entityManager;

    @EJB
    private AdministrarPersistencia administrarPersistencia;
    
    public OpLiquidarResTipo opLiquidar(OpLiquidarSolTipo msjOpLiquidarSol) throws MsjOpLiquidarFallo {

        ContextoRespuestaTipo contextoRespuesta = new ContextoRespuestaTipo();
        ContextoTransaccionalTipo contextoSolicitud = msjOpLiquidarSol.getContextoTransaccional();
        
        try {
            ldapAuthentication.validateLdapAuthentication(contextoSolicitud.getIdUsuarioAplicacion(), contextoSolicitud.getValClaveUsuarioAplicacion());
            initContextoRespuesta(contextoSolicitud, contextoRespuesta);
            
            if (msjOpLiquidarSol.getExpediente().getIdNumExpediente().equalsIgnoreCase("Reset")) {
                String result = ParentThread.resetLiquidadorPool();
                OpLiquidarResTipo liquidarResTipo = new OpLiquidarResTipo();
                contextoRespuesta.setCodEstadoTx(result);                
                liquidarResTipo.setContextoRespuesta(contextoRespuesta);
                liquidarResTipo.setEsIncumplimiento(Boolean.FALSE);
                
                LOG.error(result);
                LOG.error("Op: OpLiquidar ::: Se reinicia enviado Reset");
                
                return liquidarResTipo;
                
            } else {
                LOG.error("Op: OpLiquidar ::: INIT");
                liquidadorFacade.liquidar(msjOpLiquidarSol);
            }

        } catch (AppException ex) {
            LOG.error("ERRROR EXCEPTION opLiquidar:" + ex.getMessage(), ex);
            throw new MsjOpLiquidarFallo("Error ", ErrorUtil.buildFalloTipo(contextoRespuesta, ex), ex);
        } catch (RuntimeException ex) {
            LOG.error("ERRROR EXCEPTION opLiquidar:" + ex.getMessage(), ex);
            throw new MsjOpLiquidarFallo("Error ", ErrorUtil.buildFalloTipo(contextoRespuesta, ex), ex);
        }

        OpLiquidarResTipo liquidarResTipo = new OpLiquidarResTipo();
        // Se envia uno (1) para notificar que fue recibida la peticion
        contextoRespuesta.setCodEstadoTx("1");
        // WMRR
        liquidarResTipo.setContextoRespuesta(contextoRespuesta);
        liquidarResTipo.setEsIncumplimiento(Boolean.FALSE);

        //SrvAplLiquidadorHilo liquidadorHilo = new SrvAplLiquidadorHilo(msjOpLiquidarSol, liquidarResTipo, hojaCalculoLiquidacionDao, hojaCalculoLiquidacionDetalleDao);
        //liquidadorHilo.start();
        try {

            String stringdate = "2015-03-27 15:56:49";
            DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date = format.parse(stringdate);

            String modoEjecucion = "NOM";

            GestorProgramaService gestorProgramaService = new GestorProgramaService(administrarPersistencia,
                    entityManager, "1", modoEjecucion, date, msjOpLiquidarSol);
            gestorProgramaService.start();

        } catch (ParseException e) {
            LOG.error(e.getMessage(), e);
        }
        LOG.error("Op: OpLiquidar ::: END");
        return liquidarResTipo;

    }

}
