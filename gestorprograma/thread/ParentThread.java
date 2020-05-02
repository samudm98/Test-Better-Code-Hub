package co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.thread;

import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.OpLiquidarSolTipo;
import co.gov.ugpp.parafiscales.servicios.liquidador.entity.HojaCalculoLiquidacion;
import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.AbstractModoEjecucion;
import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.AdministrarPersistencia;
import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.entity.InRegla;
import co.gov.ugpp.parafiscales.servicios.liquidador.util.PropsReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import org.slf4j.LoggerFactory;

/**
 * @since 19/Jun/2017
 * @author UT_TECNOLOGIC
 */
public abstract class ParentThread extends Thread implements java.io.Serializable {

    private static final long serialVersionUID = 1L;
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ParentThread.class);
    private static final Object MY_SYNCHRONIZED_OBJ = new Object();
    private static final String NOMBRE = "LiquidadorPool";

    private static boolean OCUPADO = false;
    private static int POOL_SIZE = 0;

    private static Integer INI_THREAD;
    private static Integer COUNT_THREAD;

    protected static Integer TOTAL_THREAD;
    protected static Long sumaTiempos;

    protected static HojaCalculoLiquidacion HOJA_CALCULO_LIQUIDACION;
    protected static OpLiquidarSolTipo MSJ_OP_LIQUIDAR_SOL;

    protected static List<InRegla> LIST_REGLAS;
    private static List<EntityManager> LIST_EM;

    protected AdministrarPersistencia administrarPersistencia;
    protected AbstractModoEjecucion modoEjecucionService;

    /**
     * Constructor por defecto
     */
    public ParentThread() {
    }

    /**
     *
     * Constructor con parametros
     *
     * @param administrarPersistencia
     * @param modoEjecucionService
     */
    public ParentThread(AdministrarPersistencia administrarPersistencia,
            AbstractModoEjecucion modoEjecucionService) {
        this.administrarPersistencia = administrarPersistencia;
        this.modoEjecucionService = modoEjecucionService;
    }

    /**
     * Metodo para inicializar las variables estaticas
     *
     * @param hojaCalculoLiquidacion
     * @param msjOpLiquidarSol
     * @param totalThread
     * @param iniThread
     * @param listRegla
     */
    @SuppressWarnings("static-access")
    public static void setStaticValues(HojaCalculoLiquidacion hojaCalculoLiquidacion,
            OpLiquidarSolTipo msjOpLiquidarSol, Integer totalThread, Integer iniThread,
            List<InRegla> listRegla) {
        ParentThread.HOJA_CALCULO_LIQUIDACION = hojaCalculoLiquidacion;
        ParentThread.MSJ_OP_LIQUIDAR_SOL = msjOpLiquidarSol;
        ParentThread.TOTAL_THREAD = totalThread;
        ParentThread.INI_THREAD = iniThread;
        ParentThread.LIST_REGLAS = listRegla;
        ParentThread.COUNT_THREAD = 0;
        ParentThread.sumaTiempos = 0L;
        ParentThread.POOL_SIZE = Integer.parseInt(PropsReader.getKeyParam("parametroPoolSize"));
    }

    /**
     * Metodo para llevar la cuenta de hilos terminados
     */
    protected void endingThread() {
        synchronized (MY_SYNCHRONIZED_OBJ) {
            COUNT_THREAD++;
        }
    }

    /**
     * Metodo encargado de detener la tarea hasta que no se ejcuten todos los
     * hilos pendientes
     */
    @SuppressWarnings("SleepWhileInLoop")
    public void iniciarTarea() {
        // LOG.info("iniciarTarea() Start.");
        while (!isPermitido()) {
            try {
                Thread.sleep(250);
            } catch (InterruptedException ex) {
                LOG.error("InterruptedException - " + Thread.currentThread().getName(), ex);
            }
        }
        LOG.info("iniciarTarea() End.");
    }

    /**
     * Metodo encargado de hacer los llamados a los servicios de integracion
     *
     * @param IDhojaCalculoLiquidacion
     * @param Codesstado
     * @param Desestado
     * @param msjOpLiquidarSol
     */


    /**
     * Metodo encargado de determinar cuando se puede ejecutar el ultimo hilo
     *
     * @return retorna una bandera indicando que ya se puede ejecutar y realiza
     * la notificacion a los otros hilos
     */
    public boolean isPermitido() {
        boolean result = false;
        if (COUNT_THREAD.equals(TOTAL_THREAD) || COUNT_THREAD > TOTAL_THREAD) {
            result = true;
        }
        return result;
    }

    /**
     * Metodo encargado de indicar que el hilo se encuentra OCUPADO en una
     * ejecucion
     */
    public static void ocuparHilo() {
        synchronized (MY_SYNCHRONIZED_OBJ) {
            clearPool();
            ParentThread.OCUPADO = true;
        }
    }

    /**
     * Metodo encargado de cerrar los atributos estaticos al final la ejecucion
     * y principalmente indicar que ya se ecuentra disponible
     */
    public static void liberarHilo() {
        synchronized (MY_SYNCHRONIZED_OBJ) {
            setStaticValues(null, null, null, null, null);
            clearPool();
            ParentThread.OCUPADO = false;
        }
    }

    /**
     * Metodo que indica cuando puede iniciar la ejecución el pool de hilos
     *
     * @return
     */
    public static boolean isOcupado() {
        return ParentThread.OCUPADO;
    }

    /**
     * Metodo encargado de reemplazar el nombre del hilo
     */
    protected void setNewName() {
        Thread.currentThread().setName(Thread.currentThread().getName().replace("pool", NOMBRE));
    }

    /**
     * Metodo get de COUNT_THREAD
     *
     * @return
     */
    protected int getCountThreads() {
        return COUNT_THREAD;
    }

    /**
     * Metodo encargado de cargar y devolver la lista de EntityManager
     *
     * @return
     */
    private static List<EntityManager> getListEm() {
        if (LIST_EM == null) {
            LIST_EM = new ArrayList<>();
        }
        int size = LIST_EM.size();
        if (size < POOL_SIZE) {
            EntityManagerFactory emf = Persistence.createEntityManagerFactory("co.gov.ugpp.parafiscales.wsappPu");
            for (int i = size; i < POOL_SIZE; i++) {
                LIST_EM.add(emf.createEntityManager());
            }
        }
        return LIST_EM;
    }

    /**
     * Metodo encargado de buscar la entityManager en la posicion pos de la
     * lista estatica LIST_EM
     *
     * @param pos
     * @return
     */
    protected EntityManager getEntityManager(int pos) {
        EntityManager result = null;
        if (getListEm() != null && LIST_EM.size() > 0) {
            result = LIST_EM.get(pos);
            if (result == null || !result.isOpen()) {
                try {
                    LIST_EM.remove(pos);
                    // asegura que sin importar que, el objeto que devuelva siempre va ser diferente de null y va a estar abierto
                    result = getEntityManager(pos);
                } catch (Exception ex) {
                    LOG.error("Exception - getEntityManager()", ex);
                }
            }
        }
        return result;
    }

    /**
     * Metodo que limpia la lista de entityManager
     */
    public static void clearPool() {
        if (LIST_EM != null && !LIST_EM.isEmpty()) {
            for (EntityManager aux : LIST_EM) {
                try {
                    if (aux != null && aux.isOpen()) {
                        // limpiar los que no sean nulos
                        aux.clear();
                    } else {
                        // remover los nulos
                        LIST_EM.remove(aux);
                    }
                } catch (Exception ex) {
                    LOG.error("Exception - clearPool()", ex);
                }
            }
        }
    }

    /**
     * Metodo encargado de identificar procesos demonios y eliminarlos
     *
     * @return
     */
    public static boolean isDaemonLiquidadorPool() {
        List<Thread> lstThreads = getAllLiquidadorParentsThreads();
        int contaDem = 0;
        boolean result = false;
        try {
            if (isOcupado() && !lstThreads.isEmpty()) {
                for (Thread tmp : lstThreads) {
                    // eliminamos los hilos demonios
                    if (tmp != null) {
                        if (tmp.isDaemon()) {
                            contaDem++;
                            try {
                                LOG.warn("isDaemonLiquidadorPool().demonio: " + tmp.getName());
                                tmp.interrupt();
                                contaDem--;
                            } catch (SecurityException es) {
                                LOG.error("isDaemonLiquidadorPool().SecurityException: - " + tmp.getName(), es);
                            }
                        }
                    }
                }
                if (contaDem > 0) { // Solo notifica cuando no sea posible eliminar los demonios
                    result = true;
                }
            }
        } catch (Exception ex) {
            LOG.error("Error - isDaemonLiquidadorPool()", ex);
        }
        return result;
    }

    /**
     * metodo encargado de buscar y devolver todos los hilos asociados al pool
     * de liquidacion
     *
     * @return una lista con todos los hilos que coincidan
     */
    private static List<Thread> getAllLiquidadorParentsThreads() {
        Set<Thread> threadSetList = Thread.getAllStackTraces().keySet();
        List<Thread> lstThreads = new ArrayList<>();
        for (Thread tmp : threadSetList) {
            if (tmp.getName().startsWith(NOMBRE)) {
                lstThreads.add(tmp);
            }
        }
        return lstThreads;
    }

    /**
     * Metodo encargado de resetear y eliminar todo el pool de hilos de
     * liquidador y libear el pool para que pueda ser usado nuevamente
     *
     * @return
     */
    public static String resetLiquidadorPool() {
        // LOG.info("Op: resetPoolLiquidador ::: INIT");
        String result = "";
        try {
            List<Thread> lstThreads = getAllLiquidadorParentsThreads();
            int hilos = 0;
            int demonios = 0;
            int errores = 0;
            int tam = lstThreads.size();
            if (!lstThreads.isEmpty()) {
                for (Thread tmp : lstThreads) {
                    // eliminamos todos los hilos
                    if (tmp != null) {
                        try {
                            if (tmp.isDaemon()) {
                                LOG.warn("resetLiquidadorPool().demonio: " + tmp.getName());
                                demonios++;
                            } else {
                                hilos++;
                            }
                            tmp.interrupt();
                        } catch (SecurityException es) {
                            LOG.error("resetLiquidadorPool().SecurityException.demonio: - " + tmp.getName(), es);
                            errores++;
                        }
                    }
                }
            }
            result = NOMBRE + " - Hilos (" + hilos + "/" + tam + ") "
                    + " - Demonios (" + demonios + "/" + tam + ") "
                    + " - errores tratando de interrumpir (" + errores + "/" + tam + ") ";
        } catch (Exception ex) {
            LOG.error("Error - resetLiquidadorPool()", ex);
        } finally {
            liberarHilo();
            // LOG.info("Op: resetPoolLiquidador ::: END");
        }
        return result;
    }

}
