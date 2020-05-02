package co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import javax.persistence.EntityManager;
import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.OpLiquidarSolTipo;
import co.gov.ugpp.parafiscales.servicios.liquidador.entity.HojaCalculoLiquidacion;
import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.entity.InInstPrograma;
import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.entity.InRegla;
import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.jpa.GestorProgramaDao;
import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.jpa.InReglaDao;
import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.thread.MultiThread;
import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.thread.LastThread;
import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.thread.ParentThread;
// import static co.gov.ugpp.parafiscales.servicios.liquidador.srvAplLiquidador.gestorprograma.thread.ParentThread.administrarLiquidadorPool;
import co.gov.ugpp.parafiscales.servicios.liquidador.util.PropsReader;
import java.io.Serializable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.LoggerFactory;
import static co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.thread.ParentThread.isDaemonLiquidadorPool;
import co.gov.ugpp.parafiscales.servicios.liquidador.transversales.IntegracionBPM;
import co.gov.ugpp.parafiscales.servicios.liquidador.util.SQLConection;
import java.sql.SQLException;

/**
 * Clase que lanza el hilo principal para la ejecución del WS liquidador
 *
 * @author --
 */
public class GestorProgramaService extends Thread implements Serializable {

    private static final long serialVersionUID = 7038902816875760113L;
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(GestorProgramaService.class);
    private static final Object SYNCHRONIZED_OBJECT = new Object();
    private static final String NOMBRE = "LiquidadorThread";

    private EntityManager entityManager;
    private AdministrarPersistencia administrarPersistencia;
    private String idHojaCalculoLiquidacion;

    private String pIdPrograma;
    private String pModoEjecucion;
    private Date pFechaVigencia;

    private OpLiquidarSolTipo pMsjOpLiquidarSol;

    /**
     * Constructor por defecto
     */
    public GestorProgramaService() {
    }

    /**
     * Constructor con parametros
     *
     * @param administrarPersistencia
     * @param entityManager
     * @param idPrograma
     * @param modoEjecucion
     * @param fechaVigencia
     * @param msjOpLiquidarSol
     */
    public GestorProgramaService(AdministrarPersistencia administrarPersistencia, EntityManager entityManager,
            String idPrograma, String modoEjecucion, Date fechaVigencia, OpLiquidarSolTipo msjOpLiquidarSol) {
        this.entityManager = entityManager;
        this.administrarPersistencia = administrarPersistencia;
        this.pIdPrograma = idPrograma;
        this.pModoEjecucion = modoEjecucion;
        this.pFechaVigencia = fechaVigencia;
        this.pMsjOpLiquidarSol = msjOpLiquidarSol;
        this.idHojaCalculoLiquidacion = "0";
    }

    /**
     * Metodo ejecucion del hilo
     */
    @Override
    public void run() {
        Thread.currentThread().setName(Thread.currentThread().getName().replace("Thread", NOMBRE));
        synchronized (SYNCHRONIZED_OBJECT) {
            ejecutarProgramaNominaDetalleReglas(pIdPrograma, pModoEjecucion, pFechaVigencia, pMsjOpLiquidarSol);
        }
    }

    /**
     * Por cada empleado ejecuto todas las reglas
     *
     * @param idPrograma
     * @param modoEjecucion
     * @param fechaVigencia
     * @param msjOpLiquidarSol
     */
    @SuppressWarnings({"SleepWhileInLoop", "UnusedAssignment"})
    public void ejecutarProgramaNominaDetalleReglas(String idPrograma, String modoEjecucion, Date fechaVigencia,
            OpLiquidarSolTipo msjOpLiquidarSol) {
        // esperar 06 seg. a que finalice la ejecución de otro Pool de hilos
        if(isDaemonLiquidadorPool()) {
            LOG.error("#ERROR.LIQUIDADOR.DEMONIO# opLiquidar No se puede ejecutar porque existe un proceso bloqueando la ejecución.");
            try {
                Thread.currentThread().interrupt();
            } catch(SecurityException se) {
                LOG.error("opLiquidar SecurityException " + Thread.currentThread().getName(), se);
                return;
            }
        }
        while (ParentThread.isOcupado()) {
            try {
                Thread.sleep(6000);
            } catch (InterruptedException ex) {
                LOG.error("opLiquidar InterruptedException - " + Thread.currentThread().getName(), ex);
            }
        }
        ParentThread.ocuparHilo();
        InReglaDao reglaDao = new InReglaDao(getEntityManager());
        this.administrarPersistencia.setEntityManager(getEntityManager());
        int numeroFilasPilaDepurada = 0;

        GestorProgramaDao gestorProgramaDao = new GestorProgramaDao(getEntityManager());

        AbstractModoEjecucion modoEjecucionService = null;
        boolean lastThreadRun = false;
        ExecutorService executorService = null;
        try {
            if ("NOM".equals(modoEjecucion)) {

                executorService = Executors.newFixedThreadPool(
                        Integer.parseInt(PropsReader.getKeyParam("parametroPoolSize")));

                // Registrar cabecera LIQ_HOJA_CALCULO_LIQUIDACION (cabecera) - HojaCalculoLiquidacion
                HojaCalculoLiquidacion hojaCalculoLiquidacion = administrarPersistencia.registrarHojaCalculoLiquidacion(msjOpLiquidarSol);
                this.idHojaCalculoLiquidacion = hojaCalculoLiquidacion.getId().toString();

                if (msjOpLiquidarSol != null && msjOpLiquidarSol.getIdentificacion() != null
                        && msjOpLiquidarSol.getIdentificacion().getValNumeroIdentificacion() != null) {
                    numeroFilasPilaDepurada = gestorProgramaDao.verificarNitPilaDepurada(
                            msjOpLiquidarSol.getIdentificacion().getValNumeroIdentificacion());
                }

                //System.out.println("opLiquidar ejecutarProgramaNominaDetalleReglas");
                
                modoEjecucionService = new NominaModoEjecucion();

                InInstPrograma inInstPrograma = registrarInicioEjecucionInstanciaPrograma(idPrograma, msjOpLiquidarSol);

                // se obtienen los datos necesarios para ejecutar todas las reglas pertenecientes al modulo de negocio.
                List<DatosEjecucionRegla> listDatEjeRegla = modoEjecucionService.getDatosEjecucionRegla(gestorProgramaDao,
                        msjOpLiquidarSol);
                
                
                //System.out.println("opLiquidar listDatEjeRegla: " + listDatEjeRegla.isEmpty());
                //System.out.println("opLiquidar numeroFilasPilaDepurada: " + numeroFilasPilaDepurada);
                
                if (!listDatEjeRegla.isEmpty() && numeroFilasPilaDepurada > 0) 
                {

                    List<DatosEjecucionRegla> lstTemp = null;
                    List<String> lstDocumentos = new ArrayList<>();
                    // int tamanoTotal = listDatEjeRegla.size();
                    for (DatosEjecucionRegla datos : listDatEjeRegla) {
                        String doc = datos.getNominaDetalle().getNumeroIdentificacionActual().trim();
                        if (lstDocumentos.isEmpty() || !lstDocumentos.contains(doc)) {
                            //System.out.println("::ANDRES75:: doc: " + doc);
                            lstDocumentos.add(doc);
                        }
                    }
                    int sizeLstDoc = lstDocumentos.size();
                    
                    
                    //System.out.println("::ANDRES5:: size lstDocumentos: " + sizeLstDoc);
                    

                    // La lista de reglas viene ordenada segun num_order
                    List<InRegla> listRegla = reglaDao.obtenerReglasDelPrograma(idPrograma, fechaVigencia);
                    // int suma = 0;

                    //System.out.println("::ANDRES6:: size listRegla: " + listRegla.size());
                    
                    
                    //Ajuste de llamar a un nuevo procedimiento de doble linea que se ejecute
                    //antes de iniciar la liquidacion y luego se corre otro procedimiento almacenado que marca los renglones
                    //anteriores con doble linea para tenerlo en cuenta en el ibc vacaciones
                    //luego procede a ejecutar todas las reglas.
                    liquidadorDobleLineaAlInicio(hojaCalculoLiquidacion);
                    
                    
                    MultiThread.setStaticValues(hojaCalculoLiquidacion, msjOpLiquidarSol,
                            sizeLstDoc, Thread.activeCount(), listRegla);

                    
                    
                    for (String doc : lstDocumentos) {
                        lstTemp = new ArrayList<>();
                        // Se organizan los DatosEjecucionRegla para un mismo cotizante 
                        // y luego se envian cada paquete en un hilo
                        for (DatosEjecucionRegla datos : listDatEjeRegla) {
                            String doc2 = datos.getNominaDetalle().getNumeroIdentificacionActual().trim();
                            if (doc.equals(doc2)) {
                                //System.out.println("::ANDRES76:: doc2: " + doc2);
                                lstTemp.add(datos);
                            }
                        }
                        // suma += lstTemp.size();

                        
                        MultiThread mt = new MultiThread(lstTemp, modoEjecucionService, administrarPersistencia);
                        
                        //System.out.println("::ANDRES7:: size lstTemp: " + lstTemp.size());
                        
                        
                        mt.setPriority(Thread.MAX_PRIORITY);
                        executorService.submit(mt);
                        lstTemp = null;
                    }
                    // LOG.info("Total Registros: [" + suma + "/" + tamanoTotal + "]");
                    // LOG.info("Total Hilos activos: " + Thread.activeCount());
                    //administrarPersistencia.setEntityManager();
                    LastThread lt = new LastThread(modoEjecucionService, inInstPrograma, idPrograma, administrarPersistencia, getEntityManager());
                    lt.setPriority(Thread.MIN_PRIORITY);
                    executorService.submit(lt);
                    lastThreadRun = true;
                } else {
                    // TODO mandar errores no se encontraron datos para ejecutar las reglas
                    // TODO registrar error ejecucion regla
                    String identificacion = "";                
                   
                    if(msjOpLiquidarSol != null && msjOpLiquidarSol.getIdentificacion() != null)
                        identificacion = msjOpLiquidarSol.getIdentificacion().getValNumeroIdentificacion();
                    else
                        identificacion = "0";
                    
                    if(numeroFilasPilaDepurada == 0)
                          System.out.println("opLiquidar EXCEPTION ERROR. No Existe PILA para el aportante: " + identificacion + " IdHojaCalculoLiquidacion: " + this.idHojaCalculoLiquidacion);
                    
                    registrarFinEjecucionInstanciaPrograma(inInstPrograma, idPrograma, msjOpLiquidarSol);
                    
                    throw new Exception("opLiquidar EXCEPTION ERROR: " + identificacion + " IdHojaCalculoLiquidacion: " + hojaCalculoLiquidacion.getId());
                }

            }
        } catch (Exception e) {
            
            e.printStackTrace();
            
            if (executorService != null && !lastThreadRun) { // El pool de hilos ya se ha iniciado
                // hay que deterner inmediatamente el pool de hilos y liberarlo
                executorService.shutdownNow();
                executorService = null;
            } // en caso contrario o no se inicio el executor o lastThread se encarga deliberar el pool
            // en el caso que se presente un error y no se haya iniciado 
            // el pool de hilos hay que liberar el pool de hilos para que se 
            // puedan ejecutar los demas

            IntegracionBPM.servicioIntegracionRecibirLiquidacion(this.idHojaCalculoLiquidacion, "ERROR EXCEPTION", "ERROR PROCESANDO LIQUIDACION", msjOpLiquidarSol.getContextoTransaccional());
    
            ParentThread.liberarHilo();
            
        } finally {
            if (executorService != null) {
                executorService.shutdown();
            }
        }

    }

    /**
     * Metodo para obtener las reglas de negocio
     *
     * @param idPrograma
     * @param fechaVigencia
     * @return
     */
    public List<InRegla> obtenerReglasDelPrograma(String idPrograma, Date fechaVigencia) {
        InReglaDao reglaDao = new InReglaDao(getEntityManager());
        return reglaDao.obtenerReglasDelPrograma(idPrograma, fechaVigencia);
    }

    /**
     * Metodo que embebe un metodo de administrar persistencia Se obtiene el
     * identificador de instancia del programa a ejecutar y se registra la fecha
     * y hora de inicio en la tabla IN_INST_PROGRAMA.
     *
     * @param idPrograma
     * @param msjOpLiquidarSol
     * @return
     */
    public InInstPrograma registrarInicioEjecucionInstanciaPrograma(String idPrograma, OpLiquidarSolTipo msjOpLiquidarSol) {
        return administrarPersistencia.registrarInicioEjecucionInstanciaPrograma(idPrograma, msjOpLiquidarSol);
    }

    /**
     * Metodo que embebe un metodo de administrar persistencia
     *
     * @param inInstPrograma
     * @param idPrograma
     * @param msjOpLiquidarSol
     */
    public void registrarFinEjecucionInstanciaPrograma(InInstPrograma inInstPrograma, String idPrograma,
            OpLiquidarSolTipo msjOpLiquidarSol) {
        administrarPersistencia.registrarFinEjecucionInstanciaPrograma(inInstPrograma, idPrograma, msjOpLiquidarSol);
    }

    // ########################## Getter and Setter ###########################
    /**
     * Metodo get del atributo entityManager
     *
     * @return
     */
    public EntityManager getEntityManager() {
        return entityManager;
    }

    /**
     * Metodo set del atributo entityManager
     *
     * @param entityManager
     */
    public void setEntityManager(EntityManager entityManager) {
        this.entityManager = entityManager;
    }
    
    
    
    
    void liquidadorDobleLineaAlInicio(HojaCalculoLiquidacion hojaCalculoLiquidacion) {
        
        SQLConection sqlConection = null;

        try {
            sqlConection = SQLConection.getInstance();
            sqlConection.procesoDobleLineaAntesDeLiquidar(hojaCalculoLiquidacion.getId().toString());
        } catch (NullPointerException ne) {
            LOG.error("::ERROR:: liquidadorDobleLineaAlInicio ne: " + hojaCalculoLiquidacion, ne);
        } catch (Exception e) {
            LOG.error("::ERROR:: liquidadorDobleLineaAlInicio e: " + hojaCalculoLiquidacion, e);
        } finally {
            try {
                if (sqlConection != null) {
                    sqlConection.closeConnection();
                }
            } catch (SQLException ex) {
                LOG.error("::ERROR:: liquidadorDobleLineaAlInicio ex: " + hojaCalculoLiquidacion, ex);
            }

        }
    }

    

}
