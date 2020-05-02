package co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.thread;

import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.OpLiquidarSolTipo;
import co.gov.ugpp.parafiscales.servicios.liquidador.entity.HojaCalculoLiquidacion;
import co.gov.ugpp.parafiscales.servicios.liquidador.entity.PilaDepurada;
import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.AbstractModoEjecucion;
import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.AdministrarPersistencia;
import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.DatosEjecucionRegla;
import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.JavascriptRuntime;
import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.TipoScriptRuntime;
import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.entity.InRegla;
import co.gov.ugpp.parafiscales.servicios.liquidador.errortipo.v1.ErrorTipo;
import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.jpa.GestorProgramaDao;
import co.gov.ugpp.parafiscales.servicios.liquidador.transversales.IntegracionBPM;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.persistence.EntityManager;
import org.apache.commons.lang.StringUtils;
import org.slf4j.LoggerFactory;

/**
 * @since 19/Jul/2017
 * @author UT_TECNOLOGI
 */
public class MultiThread extends ParentThread {

    private static final long serialVersionUID = 1L;
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(MultiThread.class);

    static JavascriptRuntime JAVASCRIPT_RUNTIME;

    List<DatosEjecucionRegla> listDatEjeRegla;
    GestorProgramaDao gestorProgramaDao;

    /**
     * Constructor por defecto
     */
    public MultiThread() {
    }

    /**
     * Constructo de la clase con parametros
     *
     * @param listDatEjeRegla
     * @param modoEjecucionService
     * @param administrarPersistencia
     */
    public MultiThread(List<DatosEjecucionRegla> listDatEjeRegla,
            AbstractModoEjecucion modoEjecucionService, AdministrarPersistencia administrarPersistencia) {
        this.modoEjecucionService = modoEjecucionService;
        this.listDatEjeRegla = listDatEjeRegla;
        this.administrarPersistencia = administrarPersistencia;
    }

    /**
     * Metodo princiopal de ejecucion de los hilos
     */
    @Override
    public void run() {
        asignarEntityManager();
        setNewName();
        if (administrarPersistencia != null && listDatEjeRegla != null
                && HOJA_CALCULO_LIQUIDACION != null && gestorProgramaDao != null
                && LIST_REGLAS != null && modoEjecucionService != null
                && MSJ_OP_LIQUIDAR_SOL != null) {
            ejecutarListas();
            endingThread();
        } else {
            LOG.warn(Thread.currentThread().getName() + "/n>> administrarPersistencia: "
                    + administrarPersistencia + "/n>> listDatEjeRegla: " + listDatEjeRegla
                    + "/n>> hojaCalculoLiquidacion: " + HOJA_CALCULO_LIQUIDACION
                    + "/n>> gestorProgramaDao: " + gestorProgramaDao + "/n>> listRegla: "
                    + LIST_REGLAS + "/n>> modoEjecucionService: " + modoEjecucionService
                    + "/n>> msjOpLiquidarSol: " + MSJ_OP_LIQUIDAR_SOL);
        }
    }

    /**
     * Metodo encargado de realizar el proceso principal
     */
    void ejecutarListas() {
        // Long startTime = Calendar.getInstance().getTimeInMillis();
        try {
            //LOG.info("InsideThread - Cotizante: " + listDatEjeRegla.get(0).getCotizante().getTipoIdentificacion().getSigla()
            //        + "-" + listDatEjeRegla.get(0).getCotizante().getNumeroIdentificacion()
            //        + " => Registros: " + listDatEjeRegla.size());
            for (DatosEjecucionRegla datosEjecuRegla : listDatEjeRegla) {
                List<ErrorTipo> errorTipoList = new ArrayList<>();
                // se guardan los resultados para las reglas que requieren variables adicionales y busqueda a la base de datos
                Map<String, Object> variablesRegla = new HashMap<>();

                // mapa cache para guardar pila depurada
                Map<String, Object> mapPilaDepurada = new HashMap<>();
                
                
                
                
                /*
                if(datosEjecuRegla.getCotizante().getNumeroIdentificacion().equals("1000393339"))
                        System.out.println("::ANDRES8:: size mapPilaDepurada: " + mapPilaDepurada.size());
                */
                 

                Map<String, Object> infoNegocio = new HashMap<>();
                infoNegocio.put("IDHOJACALCULOLIQUIDACION", String.valueOf(HOJA_CALCULO_LIQUIDACION.getId()));

                // por cada empleado se obtiene un pila depurada
                PilaDepurada pilaDepurada = gestorProgramaDao.obtegerPilaDepuradaNominaDetalleCache(mapPilaDepurada, datosEjecuRegla.getNomina(),
                        datosEjecuRegla.getNominaDetalle());
                
                
                /*
                if(datosEjecuRegla.getCotizante().getNumeroIdentificacion().equals("1000393339"))
                    System.out.println("::ANDRES9:: ID pilaDepurada: " + pilaDepurada.getId());
                */

                // TODO PROMEDIO 1
                for (InRegla regla : LIST_REGLAS) {
                    try {
                        String valScr = new String(regla.getValScript());
                        
                        /*
                        if(regla.getNombre().equals("TOTAL_REMUNERADO") && 
                                datosEjecuRegla.getCotizante().getNumeroIdentificacion().equals("1000393339") &&
                                    datosEjecuRegla.getNominaDetalle().getAno().intValue() == 2013 &&
                                          datosEjecuRegla.getNominaDetalle().getMes().intValue() == 10)
                        {       
                            System.out.println("::ANDRES10:: getNomina: " + datosEjecuRegla.getNomina().getId());
                            System.out.println("::ANDRES10:: getNominaDetalle: " + datosEjecuRegla.getNominaDetalle().getId());
                            System.out.println("::ANDRES10:: getNominaDetalle: " + datosEjecuRegla.getCotizante().getId());
                            System.out.println("::ANDRES10:: ID regla: " + regla.getId());
                            System.out.println("::ANDRES10:: SCRIPT regla: " + valScr);
                            System.out.println("::ANDRES10:: ID pilaDepurada: " + pilaDepurada.getId());
                            System.out.println("::ANDRES10:: size mapPilaDepurada: " + mapPilaDepurada.size());
                            System.out.println("::ANDRES10:: size infoNegocio: " + infoNegocio.size());
                            System.out.println("::ANDRES10:: size variablesRegla: " + variablesRegla.size());
                        }
                        */

                        ejecutarReglaNominaDetalle(mapPilaDepurada, gestorProgramaDao, modoEjecucionService,
                                datosEjecuRegla, regla, valScr, MSJ_OP_LIQUIDAR_SOL, infoNegocio, errorTipoList,
                                variablesRegla, pilaDepurada);
                    } catch (Exception e) {
                        //LOG.error("ERROR LOOP CEDULA --> REGLAS");
                        //LOG.error(e.getMessage());
                        //llamar_servicioIntegracion(0, "ERROR", "ERROR LOOP CEDULA --> REGLAS", MSJ_OP_LIQUIDAR_SOL);  
                        LOG.error("opLiquidar EXCEPTION ERROR EN CEDULA IdHojaCalculoLiquidacion: " + String.valueOf(HOJA_CALCULO_LIQUIDACION.getId()));                        
                        IntegracionBPM.servicioIntegracionRecibirLiquidacion(HOJA_CALCULO_LIQUIDACION.getId().toString(), "ERROR EXCEPTION", "ERROR EN CEDULA", MSJ_OP_LIQUIDAR_SOL.getContextoTransaccional());
                        
                        throw new Exception("opLiquidar EXCEPTION ERROR EN CEDULA IdHojaCalculoLiquidacion: " + HOJA_CALCULO_LIQUIDACION.getId());   
                    }
                }
                // TODO PROMEDIO 2
                
                
                //datosEjecuRegla
                        
                        
                
                modoEjecucionService.procesarReglasNoFormula(errorTipoList, gestorProgramaDao, datosEjecuRegla, infoNegocio, pilaDepurada);
                
                
                
                
                
                //TOTAL_REMUNERADO
                
                //if(datosEjecuRegla.getCotizante().getNumeroIdentificacion().equals("1000393339"))
                //{  
                //    System.out.println("::ANDRES15:: (TOTAL_REMUNERADO,1000393339,"+ datosEjecuRegla.getNominaDetalle().getMes()  +") infonegocio: " + infoNegocio.get("1000393339#TOTAL_REMUNERADO#201310") + "-" + infoNegocio.get("1000393339#TOTAL_REMUNERADO#201311") + "-" + infoNegocio.get("1000393339#TOTAL_REMUNERADO#201312"));
                //}

                
               /* 1000393339#TOTAL_REMUNERADO#201310
                    Iterator iterator = infoNegocio.keySet().iterator();
                    while (iterator.hasNext()) {
                        String key = iterator.next().toString();
                        String value = infoNegocio.get(key).toString();
                        System.out.println("::ANDRES11:: " + key + " " + value);
                    } 
                */
                
                /*
                 if(datosEjecuRegla.getCotizante().getNumeroIdentificacion().equals("1000393339") &&
                                    datosEjecuRegla.getNominaDetalle().getAno().intValue() == 2013 &&
                                          datosEjecuRegla.getNominaDetalle().getMes().intValue() == 10)
                */
                
                
               
                administrarPersistencia.guardarResultadoReglaNominaDetalle(datosEjecuRegla, LIST_REGLAS, MSJ_OP_LIQUIDAR_SOL,
                        HOJA_CALCULO_LIQUIDACION, infoNegocio);



                
                mapPilaDepurada.clear();
                errorTipoList.clear();
                variablesRegla.clear();
                infoNegocio.clear();
            }
            //LOG.info("FIN PROCESO POR CEDULA. SE PROCESO TODAS LAS REGLAS : "
            //        + listDatEjeRegla.get(0).getCotizante().getNumeroIdentificacion());

        } catch (ExceptionInInitializerError ex) {
            LOG.error("ExceptionInInitializerError - " + Thread.currentThread().getName(), ex);
        } catch (Exception e) {
            LOG.error("Exception - " + Thread.currentThread().getName(), e);
        } finally {
            //Long diferencia = (Calendar.getInstance().getTimeInMillis() - startTime);
            //synchronized (MY_SYNCHRONIZED_OBJ) {
            //    ParentThread.sumaTiempos += diferencia;
            //}
            //int contador = getCountThreads();
            //LOG.info("Cotizante: " + listDatEjeRegla.get(0).getCotizante().getTipoIdentificacion().getSigla()
            //        + "-" + listDatEjeRegla.get(0).getCotizante().getNumeroIdentificacion()
            //        + " => " + Thread.currentThread().getName() + " (" + (contador + 1)
            //        + "/" + ParentThread.TOTAL_THREAD + ") End.");
            gestorProgramaDao = null;
            administrarPersistencia = null;
            modoEjecucionService = null;
            listDatEjeRegla = null;
        }
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
    public static void setStaticValues(HojaCalculoLiquidacion hojaCalculoLiquidacion,
            OpLiquidarSolTipo msjOpLiquidarSol, Integer totalThread, Integer iniThread, List<InRegla> listRegla) {
        //LOG.info("thread.MultiThread.setStaticValues() - : LIST_REGLAS.size(): "
        //        + ((LIST_REGLAS != null) ? LIST_REGLAS.size() : 0));
        ParentThread.setStaticValues(hojaCalculoLiquidacion, msjOpLiquidarSol, totalThread, iniThread, listRegla);
        MultiThread.JAVASCRIPT_RUNTIME = null;
    }

    /**
     * Metodo para ejecutar el detalle de la nomina
     *
     * @param mapPilaDepurada
     * @param gestorProgramaDao
     * @param modoEjecucionService
     * @param obj
     * @param inRegla
     * @param scriptRegla
     * @param msjOpLiquidarSol
     * @param infoNegocio
     * @param errorTipo
     * @param mapVariablesRegla
     * @param pilaDepurada
     */
    public void ejecutarReglaNominaDetalle(Map<String, Object> mapPilaDepurada, GestorProgramaDao gestorProgramaDao,
            AbstractModoEjecucion modoEjecucionService, DatosEjecucionRegla obj, InRegla inRegla, String scriptRegla,
            OpLiquidarSolTipo msjOpLiquidarSol, Map<String, Object> infoNegocio, List<ErrorTipo> errorTipo,
            Map<String, Object> mapVariablesRegla, PilaDepurada pilaDepurada) {

        try {
            // antes de ejecutar regla verifico si esa regla requiere que busque informacion en otra parte
            // tambien me debe decir si envia un resultado prefabricado (variable) o simplemente ejecuto la regla con el
            // groovy/javascript
            Object resultRegla = modoEjecucionService.buscarVariablesRegla(errorTipo, gestorProgramaDao, obj, inRegla,
                    mapVariablesRegla);

            /*
                        if(inRegla.getNombre().equals("TOTAL_REMUNERADO") && 
                                obj.getCotizante().getNumeroIdentificacion().equals("1000393339") &&
                                    obj.getNominaDetalle().getAno().intValue() == 2013 &&
                                          obj.getNominaDetalle().getMes().intValue() == 10)
                        {       
                            System.out.println("::ANDRES10:: resultRegla: " + resultRegla);
                        }
            */
            
            // si es null fue posible encontrar el valor que requeria la regla para poder ejecutar la formula
            // TODO verificar con wilson
            if (resultRegla == null) {

                String script = modoEjecucionService.inyectarValoresRegla(scriptRegla, obj, mapVariablesRegla);

                script = modoEjecucionService.reemplazarVariablesRegla(script, obj, mapVariablesRegla);

                script = procesarValoresRegla(obj, obj.getNominaDetalle().getNumeroIdentificacionActual() == null ? null : obj.getNominaDetalle().getNumeroIdentificacionActual(), infoNegocio, script);

                // modoEjecucionService.procesarReglasNoFormula(errorTipo, gestorProgramaDao, obj, infoNegocio, pilaDepurada);
                if (StringUtils.containsAny("nil", script)) {
                    ErrorTipo errorObj = new ErrorTipo();
                    errorObj.setCodError(inRegla.getCodigo());
                    errorObj.setValDescError("Error al evaluar regla.");
                    errorObj.setValDescErrorTecnico(script);
                    errorTipo.add(errorObj);
                } else {
                    if (StringUtils.isNotEmpty(script)) {
                        if (!"-".equals(script)) {
                            
                            Double valor = evaluarScript(inRegla, script);
                            //System.out.println("::ANDRES10:: getId: " + inRegla.getId());

                            try {
                                // se pregunta por nombre de regla para hacer redondeo de resultado
                                if ("COTIZ_OBL_CALCULADA_SALUD".equals(inRegla.getCodigo())) {
                                    valor = modoEjecucionService.roundValor100(new BigDecimal(valor)).doubleValue();
                                    //System.out.println("::ANDRES11:: getId: " + inRegla.getId());
                                } else if ("AJUSTE_SALUD".equals(inRegla.getCodigo())) {
                                    valor = modoEjecucionService.roundValor100(new BigDecimal(valor)).doubleValue();
                                    //System.out.println("::ANDRES12:: getId: " + inRegla.getId());
                                }
                            } catch (Exception e) {
                                LOG.error("Exception", e);
                            }
                            
                            
                            if(valor.isNaN())
                                valor = Double.parseDouble("0");
                            
                            /*
                            System.out.println("::ANDRES13:: getId: " + inRegla.getId());
                            System.out.println("::ANDRES13:: script: " + script);
                            System.out.println("::ANDRES13:: valor: " + valor);
                            System.out.println("::ANDRES13:: getNumeroIdentificacion: " + obj.getCotizante().getNumeroIdentificacion());
                            System.out.println("::ANDRES13:: getCodigo: " + inRegla.getCodigo());
                            System.out.println("::ANDRES13:: modoEjecucionService: " + modoEjecucionService.anyoMesDetalleKey(obj));
                            System.out.println("::ANDRES13:: toString: " + Double.toString(valor));
                            System.out.println("::ANDRES13:: BigDecimal: " + (new BigDecimal(Double.toString(valor))));
                            */
                            
                            
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#" + inRegla.getCodigo()
                                    + modoEjecucionService.anyoMesDetalleKey(obj), new BigDecimal(Double.toString(valor)));
                        }
                    }
                }

            } // para todas las reglas que regresen este valor se guarda cero
            else if ("-".equals(resultRegla)) {
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#" + inRegla.getCodigo()
                        + modoEjecucionService.anyoMesDetalleKey(obj), new BigDecimal("0"));
            }
        } catch (Exception e) {
            // TODO cuando no apliquemos regla xq una de las variables es null se mete a la lista de errores
            // registrar el error de ejecutar la regla para una cedula
            LOG.error("ERROR AL EJECUTAR REGLA " + inRegla.getCodigo() + ":" + modoEjecucionService.anyoMesDetalleKey(obj)
                    + " ,CEDULA " + obj.getNominaDetalle().getNumeroIdentificacionActual() + " , NUMERODETALLE:"
                    + obj.getNominaDetalle().getId());
            
            //e.printStackTrace();
            
            LOG.error(null, e);
        }

    }

    /**
     * Metodo para ejecutar los scrips asincronos
     *
     * @param regla
     * @param script
     * @return
     * @throws Exception
     */
    public Double evaluarScript(InRegla regla, String script) throws Exception {
        Double result = null;

        if (TipoScriptRuntime.JAVASCRIPT.equals(regla.getCod_tipo())) {
            if (JAVASCRIPT_RUNTIME == null) {
                JAVASCRIPT_RUNTIME = new JavascriptRuntime();
            }

            result = JAVASCRIPT_RUNTIME.ejecutarScript(script);
        }
        return result;
    }

    /**
     * Verifica si las posibles variables de tipo regla que necesita el script
     * para ejecutarse se encuentra en el mapa
     *
     * @param obj
     * @param cedula
     * @param infoNegocio
     * @param scriptRegla
     * @return
     */
    public String procesarValoresRegla(DatosEjecucionRegla obj, String cedula, Map<String, Object> infoNegocio, String scriptRegla) {
        @SuppressWarnings("RedundantStringConstructorCall")
        String result = new String(scriptRegla);

        for (Map.Entry<String, Object> pair : infoNegocio.entrySet()) {
            String[] reglaCotizante = StringUtils.splitByWholeSeparator((String) pair.getKey(), "#");

            if (pair.getValue() instanceof Number) {
                // FIXME verificar año mes
                if (reglaCotizante[0].equals(cedula)
                        && reglaCotizante[2].equals(obj.getNominaDetalle().getAno().toString()
                                + obj.getNominaDetalle().getMes().toString())) {
                    if (StringUtils.containsIgnoreCase(result, "{" + reglaCotizante[1] + "}")) {
                        result = StringUtils.replace(result, "{" + reglaCotizante[1] + "}", pair.getValue().toString());
                    }

                }
            }
        }
        return result;
    }

    /**
     * Metodo encargado de encontrar y devolver el identificador del hilo
     * asignado
     *
     * @return
     */
    private int getIdThread() {
        String[] arrayName = Thread.currentThread().getName().split("-");
        String sId = arrayName[arrayName.length - 1];
        int id = (Integer.valueOf(sId) - 1);
        return id;
    }

    /**
     * Metodo encargado de asignar las EntityManager correspondiente para su
     * ejecucion
     */
    protected void asignarEntityManager() {
        EntityManager em = getEntityManager(getIdThread());
        gestorProgramaDao = new GestorProgramaDao(em);
        administrarPersistencia.setEntityManager(em);
    }
    
    
    
    
    
    
    
    

}
