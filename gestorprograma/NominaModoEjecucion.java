package co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.OpLiquidarSolTipo;
import co.gov.ugpp.parafiscales.servicios.liquidador.entity.AportanteLIQ;
import co.gov.ugpp.parafiscales.servicios.liquidador.entity.CobFlex;
import co.gov.ugpp.parafiscales.servicios.liquidador.entity.CobParamGeneral;
import co.gov.ugpp.parafiscales.servicios.liquidador.entity.HojaCalculoLiquidacionDetalle;
import co.gov.ugpp.parafiscales.servicios.liquidador.entity.Nomina;
import co.gov.ugpp.parafiscales.servicios.liquidador.entity.NominaDetalle;
import co.gov.ugpp.parafiscales.servicios.liquidador.entity.PilaDepurada;
import co.gov.ugpp.parafiscales.servicios.liquidador.errortipo.v1.ErrorTipo;
import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.entity.InRegla;
import co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion.gestorprograma.jpa.GestorProgramaDao;
import co.gov.ugpp.parafiscales.servicios.liquidador.util.CacheService;
import static java.lang.Thread.sleep;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.slf4j.LoggerFactory;

public class NominaModoEjecucion extends AbstractModoEjecucion {

    private static final long serialVersionUID = -2979443613854785520L;

    CobParamGeneral cobParamGeneral = new CobParamGeneral();
    //CobSbsis cobSbsis = new CobSbsis();

    static Map<String, String> LST_ADMINISTRADORA_PILA = null;
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(NominaModoEjecucion.class);

    @Override
    public List<DatosEjecucionRegla> getDatosEjecucionRegla(GestorProgramaDao gestorProgramaDao, OpLiquidarSolTipo msjOpLiquidarSol) {
        

        //System.out.println("Exp:"+msjOpLiquidarSol.getExpediente().getIdNumExpediente());
        
        List<Nomina> nominaList = gestorProgramaDao.nominaByIdExpediente(msjOpLiquidarSol.getExpediente().getIdNumExpediente());

        
        //System.out.println("::ANDRES1:: Nomina size nominas: " + nominaList.size());

        // se trae la informacion de la ultima nomina
        
        //OJO AQUI SE ESTA DESBORDANDO PROQUE NO GUARDO LA NOMINA
        //        MIRAR PROQUE NO LA GUARDO PERO PRIMERO CORREGIR
        //                QUE SI VALE 0 NO SE ESTALLE SINO QUE RESPONDA CON 
        //                        EL LLAMADO AL BPM SINO SE VA A QUEDAR ASI SIN RESPONDER
        //                                CONSUMIENDO RECURSOS QUE SUELTE QUE SUELTE SIEMPRE
        //                                        ANTE CUALQUIER ERROR
        //                                                METER ESE AJUSTE PRIMERO Y HAY SI LUEGO MIRAR
        //                                                        PORQUE NO GUARDO LA NOMINA QUE SE ENVIO
        List<NominaDetalle> nomDetList = null;
        List<DatosEjecucionRegla> listDatEjeRegla = new ArrayList<>();
        if(nominaList.size() > 0)
            nomDetList = gestorProgramaDao.nominaDetalleByIdNomina(nominaList.get(0));
        else {
            Logger.getLogger(NominaModoEjecucion.class.getName()).log(Level.SEVERE,"GetDatosEjecucionRegla: Error Exception no se encontro NOMINA a procesar.");
            return listDatEjeRegla;
        }
        cargarLstAdministradoraPila(gestorProgramaDao);
        boolean primerReg = false;
        String nit = "";
       
        //System.out.println("::ANDRES2:: Nomina size nomina detalle: " + nomDetList.size());

        for (NominaDetalle obj : nomDetList) {
            //System.out.println("::ANDRES2:: cotLiq: " + cotLiq.getNumeroIdentificacion());
            //System.out.println("::ANDRES3:: primerReg: " + primerReg);
            //System.out.println("::ANDRES4:: nominaList: " + nominaList.get(0).getId());
            if (!primerReg) {
                AportanteLIQ aportanteLiq = gestorProgramaDao.aportanteLIQById(obj);
                nit = aportanteLiq.getNumeroIdentificacion();
                //System.out.println("::ANDRES5:: nit: " + nit);
                primerReg = true;
                //System.out.println("::ANDRES6:: primerReg: " + primerReg);
            }
            DatosEjecucionRegla ejecucionRegla = new DatosEjecucionRegla();
            ejecucionRegla.setNomina(nominaList.get(0));
            ejecucionRegla.getNomina().setNit(new BigInteger(nit));
            //System.out.println("::ANDRES7:: setNit: " + ejecucionRegla.getNomina().getNit());
            ejecucionRegla.setNominaDetalle(obj);
            listDatEjeRegla.add(ejecucionRegla);
        }
        //System.out.println("::ANDRES3:: Nomina size lista reglas: " + listDatEjeRegla.size());
        return listDatEjeRegla;
    }

    /**
     * Variables que deben ser reemplazadas por sus valores en los objetos java
     *
     * @return
     */
    @Override
    @SuppressWarnings("RedundantStringConstructorCall")
    public String inyectarValoresRegla(String scriptRegla, DatosEjecucionRegla obj, Map<String, Object> mapVariablesRegla) {
        String script = "";

        // FIXME solucionar error para que busque palabras completas
        if (StringUtils.contains(scriptRegla, "{DIAS_TRABAJADOS_MES}")) {
            script = StringUtils.replace(scriptRegla, "{DIAS_TRABAJADOS_MES}", getNumberToString(obj.getNominaDetalle().getDiasTrabajadosMes()));
        } else {
            script = new String(scriptRegla);
        }
        if (StringUtils.contains(script, "{DIA_LICEN_REMUNERADAS_MES}")) {
            script = StringUtils.replace(script, "{DIA_LICEN_REMUNERADAS_MES}", getNumberToString(obj.getNominaDetalle().getDiasLicenciaRemuneradasMes()));
        }
        if (StringUtils.contains(script, "{DIAS_SUSPENSION_MES}")) {
            script = StringUtils.replace(script, "{DIAS_SUSPENSION_MES}",
                    getNumberToString(obj.getNominaDetalle()
                            .getDiasSuspensionMes()));
        }
        if (StringUtils.contains(script, "{DIAS_HUELGA_LEGAL_MES}")) {
            script = StringUtils.replace(script, "{DIAS_HUELGA_LEGAL_MES}", getNumberToString(obj.getNominaDetalle().getDiasHuelgaLegalMes()));
        }
        if (StringUtils.contains(script, "{DIAS_INCAPACIDADES_MES}")) {
            script = StringUtils.replace(script, "{DIAS_INCAPACIDADES_MES}", getNumberToString(obj.getNominaDetalle().getDiasIncapacidadesMes()));
        }
        if (StringUtils.contains(script, "{DIAS_VACACIONES_MES}")) {
            script = StringUtils.replace(script, "{DIAS_VACACIONES_MES}", getNumberToString(obj.getNominaDetalle().getDiasVacacionesMes()));
        }
        if (StringUtils.contains(script, "{DIA_LICEN_MATERNI_PATERNI_MES}")) {
            script = StringUtils.replace(script, "{DIA_LICEN_MATERNI_PATERNI_MES}", getNumberToString(obj.getNominaDetalle().getDiasLicenciaMaternidadPaternidadMes()));
        }
        //modificado andres 15/06/2017
        if (StringUtils.contains(script, "{IBCVACACIONES}"))
	    script = StringUtils.replace(script,"{IBCVACACIONES}", mapVariablesRegla.get("IBCVACACIONES#" + obj.getNominaDetalle().getNumeroIdentificacionActual() + anyoMesDetalleKey(obj)).toString());
        return script;
    }

    /**
     * Se hacee busqueda de variables necesitadas por las reglas antes de
     * ejecutar la regla
     *
     * @param errorTipo
     * @param gestorProgramaDao
     * @param obj
     * @param inRegla
     * @param mapVariablesRegla
     * @return
     */
    @Override
    public Object buscarVariablesRegla(List<ErrorTipo> errorTipo, GestorProgramaDao gestorProgramaDao, DatosEjecucionRegla obj,
            InRegla inRegla, Map<String, Object> mapVariablesRegla) {

        switch (inRegla.getCodigo()) {
            case "IBC_PERMISOS_REMUNERADOS":
                if (obj.getNominaDetalle().getDiasLicenciaRemuneradasMes() != null && obj.getNominaDetalle().getDiasLicenciaRemuneradasMes() > 0) {
                    String totalDiasAnteriorKey = "TOTAL_DIAS_REPORTADOS_MES_ANTERIOR#" + obj.getNominaDetalle().getNumeroIdentificacionActual() + anyoMesDetalleKey(obj);
                    if (!mapVariablesRegla.containsKey(totalDiasAnteriorKey)) {
                        mapVariablesRegla.put(totalDiasAnteriorKey, obtenerTotalDiasReportadosMesAnterior(gestorProgramaDao, obj));
                    }
                    String key = "IBC#" + obj.getNominaDetalle().getNumeroIdentificacionActual() + anyoMesDetalleKey(obj);
                    if (!mapVariablesRegla.containsKey(key)) {
                        Integer ibc = 0;
                        if(!obj.getNominaDetalle().getTipoCotizante().equals("31"))   
                            ibc = obtenerIBC(gestorProgramaDao, obj, mapVariablesRegla);
                        else
                            ibc = 0;

                        if (ibc != null) {
                            mapVariablesRegla.put(key, ibc);
                        } else {
                            return "-";
                        }
                    }
                } else {
                    return "-";
                }
                break;
            case "IBC_SUSP_PERMISOS":
                if (obj.getNominaDetalle().getDiasSuspensionMes() != null && obj.getNominaDetalle().getDiasSuspensionMes() > 0) {
                    String totalDiasAnteriorKey = "TOTAL_DIAS_REPORTADOS_MES_ANTERIOR#" + obj.getNominaDetalle().getNumeroIdentificacionActual() + anyoMesDetalleKey(obj);
                    if (!mapVariablesRegla.containsKey(totalDiasAnteriorKey)) {
                        mapVariablesRegla.put(totalDiasAnteriorKey, obtenerTotalDiasReportadosMesAnterior(gestorProgramaDao, obj));
                    }
                    String key = "IBC#" + obj.getNominaDetalle().getNumeroIdentificacionActual() + anyoMesDetalleKey(obj);
                    if (!mapVariablesRegla.containsKey(key)) {
                        Integer ibc = 0;
                        if(!obj.getNominaDetalle().getTipoCotizante().equals("31")) 
                            ibc = obtenerIBC(gestorProgramaDao, obj, mapVariablesRegla);
                        else
                            ibc = 0;

                        if (ibc != null) {
                            mapVariablesRegla.put(key, ibc);
                        } else {
                            return "-";
                        }
                    }
                } else {
                    return "-";
                }
                break;
            case "IBC_VACACIONES":
                
                    //System.out.println("::ANDRES60:: vacaciones anyoMesDetalleKey: " + anyoMesDetalleKey(obj) + " ::MES:: " + anyoMesDetalleKey(obj));
                    //break;
                
                    //Se verifica si en el campo "número DE DÍAS VACACIONES DISFRUTADAS EN EL MES"  es igual a cero (0), 
                    //en tal caso no se hace ningún calculo, caso contrario, en que sea mayor a cero (0),
                    if (obj.getNominaDetalle().getDiasVacacionesMes() != null && obj.getNominaDetalle().getDiasVacacionesMes() > 0) {
                        /*
                        if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 6)
                                System.out.println("::ANDRES01:: key getDiasVacacionesMes: " + obj.getNominaDetalle().getDiasVacacionesMes());	
                        */
                        String totalDiasAnteriorKey = "TOTAL_DIAS_REPORTADOS_MES_ANTERIOR#" + obj.getNominaDetalle().getNumeroIdentificacionActual() + anyoMesDetalleKey(obj);
                        /*
                        if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 6)
                            System.out.println("::ANDRES02:: key totalDiasAnteriorKey: " + totalDiasAnteriorKey);
                        */
			if (!mapVariablesRegla.containsKey(totalDiasAnteriorKey)) {
                            mapVariablesRegla.put(totalDiasAnteriorKey, obtenerTotalDiasReportadosMesAnterior(gestorProgramaDao, obj));
                            /*
                            if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 6)
                                System.out.println("::ANDRES03:: valor totalDiasAnteriorKey: " + mapVariablesRegla.get(totalDiasAnteriorKey));
                            */
                        }
                        String key1 = "IBC#" + obj.getNominaDetalle().getNumeroIdentificacionActual() + anyoMesDetalleKey(obj);
                        /*
                        if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 6)
                            System.out.println("::ANDRES04:: valor key1: " + key1);
                        */
			if (!mapVariablesRegla.containsKey(key1)) {
                            Integer ibc1 = 0;
                            if(!obj.getNominaDetalle().getTipoCotizante().equals("31")) {    
                                ibc1 = obtenerIBC(gestorProgramaDao, obj, mapVariablesRegla);
                                /*
                                if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 6)
                                       System.out.println("::ANDRES05:: valor ibc1: " + ibc1);
                                */
                            }else{    
                                /*
                                if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 6)
                                   System.out.println("::ANDRES06:: valor ibc1: 0");
                                */
                                ibc1 = 0;
                            }
                            if (ibc1 != null){   
                                /*
                                if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 6)
                                    System.out.println("::ANDRES07:: valor key1: " + key1 + " * " + " IBC: " + ibc1);
				*/
                                mapVariablesRegla.put(key1, ibc1);
                            } else {
                                /*
                                if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 6)
                                    System.out.println("::ANDRES08:: se devuelve: - ");
                                */
                                return "-";
                            }
			}
                        String key2 = "IBCVACACIONES#" + obj.getNominaDetalle().getNumeroIdentificacionActual() + anyoMesDetalleKey(obj);
                        /*
                        if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 6)
                            System.out.println("::ANDRES09:: key: " + key2 + " ::MES:: " + anyoMesDetalleKey(obj));
                        */
			if (!mapVariablesRegla.containsKey(key2)){
                            //INICIO DE REGLA NUEVA
                            BigInteger ibc2 = BigInteger.ZERO;
                            if(!obj.getNominaDetalle().getTipoCotizante().equals("31")){   
                                ibc2 = obtenerIBCvacaciones(gestorProgramaDao, obj);
                                /*
                                if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 6)
                                    System.out.println("::ANDRES10:: obtenerIBCvacaciones: " + ibc2);
                                */
                            }else{
                                ibc2 = BigInteger.ZERO;
                                /*
                                if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 6)
                                    System.out.println("::ANDRES11:: ibc2: " + ibc2);
                                */
                            }           
                            if (ibc2 != null){
                                /*
                                if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 6)
                                {
                                    System.out.println("::ANDRES12:: key2: " + key2 + " ::MES:: " + anyoMesDetalleKey(obj));
                                    System.out.println("::ANDRES13:: ibc2: " + ibc2 + " ::MES:: " + anyoMesDetalleKey(obj));
                                }
                                */
                                mapVariablesRegla.put(key2, ibc2);
                            }else{
                                /*
                                if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 6)
                                    System.out.println("::ANDRES14:: se devuelve: - ");
                                */
                                return "-";
			    }
			}

                    } else {
                        /*
                        if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 6)
                            System.out.println("::ANDRES15:: devolvi: - ");         
                        */
                        return "-";
                    }
                        
                break;
            case "IBC_HUELGA":
                if (obj.getNominaDetalle().getDiasHuelgaLegalMes() != null && obj.getNominaDetalle().getDiasHuelgaLegalMes() > 0) {
                    String totalDiasAnteriorKey = "TOTAL_DIAS_REPORTADOS_MES_ANTERIOR#" + obj.getNominaDetalle().getNumeroIdentificacionActual() + anyoMesDetalleKey(obj);
                    if (!mapVariablesRegla.containsKey(totalDiasAnteriorKey)) {
                        mapVariablesRegla.put(totalDiasAnteriorKey, obtenerTotalDiasReportadosMesAnterior(gestorProgramaDao, obj));
                    }
                    String key = "IBC#" + obj.getNominaDetalle().getNumeroIdentificacionActual() + anyoMesDetalleKey(obj);
                    if (!mapVariablesRegla.containsKey(key)) {
                        Integer ibc = obtenerIBC(gestorProgramaDao, obj, mapVariablesRegla);
                        if (ibc != null) {
                            mapVariablesRegla.put(key, ibc);
                        } else {
                            return "-";
                        }
                    }

                } else {
                    return "-";
                }
                break;
            default:
                break;
        }

        return null;
    }

    @Override
    public String reemplazarVariablesRegla(String scriptRegla, DatosEjecucionRegla obj, Map<String, Object> mapVariablesRegla) {
        // se debe tener encuenta la cedula para buscar el valor por cedula del
        // cliente
        Iterator<?> it = mapVariablesRegla.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();

            if (StringUtils.contains((String) pair.getKey(), "#")) {
                String[] variableCedula = StringUtils.splitByWholeSeparator((String) pair.getKey(), "#");

                // FIXME resultado de una regla esta compuesto por
                // REGLA#CEDULA#ANOMES
                if (StringUtils.equals(variableCedula[1], obj.getNominaDetalle().getNumeroIdentificacionActual())
                        && variableCedula[2].equals(obj.getNominaDetalle().getAno().toString() + obj.getNominaDetalle().getMes().toString())) {
                    if (StringUtils.contains(scriptRegla, "{" + variableCedula[0] + "}")) {
                        // tocaria desgranar el codigo para obtener la variable
                        if (pair.getValue() instanceof Number) {
                            scriptRegla = StringUtils.replace(scriptRegla, "{" + variableCedula[0] + "}",
                                    getNumberToString((Number) pair.getValue()));
                        } else if (pair.getValue() instanceof String) {
                            scriptRegla = StringUtils.replace(scriptRegla, "{" + variableCedula[0] + "}", (String) pair.getValue());
                        }
                    }
                }
            } else {
                if (StringUtils.contains(scriptRegla, (String) pair.getKey())) {
                    if (pair.getValue() instanceof Number) {
                        scriptRegla = StringUtils.replace(scriptRegla, (String) pair.getKey(), getNumberToString((Number) pair.getValue()));
                    } else if (pair.getValue() instanceof String) {
                        scriptRegla = StringUtils.replace(scriptRegla, (String) pair.getKey(), (String) pair.getValue());
                    }
                }
            }

        }

        return scriptRegla;
    }

    private Integer obtenerIBC(GestorProgramaDao gestorProgramaDao, DatosEjecucionRegla obj, Map<String, Object> mapVariablesRegla) {

        HojaCalculoLiquidacionDetalle hojaDetalle = gestorProgramaDao.obtenerHojaCalculoLiquidacionDetalle(obj.getNomina(), obj.getNominaDetalle());
                /*
                if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 6)
                {
                    System.out.println("::ANDRES16:: getNumeroIdentificacion: " + obj.getNominaDetalle().getNumeroIdentificacionActual());
                    System.out.println("::ANDRES17:: anyoMesDetalleKey: " + anyoMesDetalleKey(obj));
                    System.out.println("::ANDRES18:: getNominaDetalle: " + obj.getNominaDetalle().getId());
                    System.out.println("::ANDRES19:: getNit: " + obj.getNomina().getNit());
                    System.out.println("::ANDRES20:: getNumeroIdentificacion: " + obj.getNominaDetalle().getNumeroIdentificacionActual());
                    System.out.println("::ANDRES21:: menos 1 de getAno: " + obj.getNominaDetalle().getAno().intValue());
                    System.out.println("::ANDRES22:: menos 1 de getMes: " + obj.getNominaDetalle().getMes().intValue());
                    System.out.println("::ANDRES23:: hojaDetalle: " + hojaDetalle);
                    if(hojaDetalle != null)
                    {
                        if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 6)
                            System.out.println("::ANDRES24:: hojaDetalle: " + hojaDetalle.getId());
                    }
                }
                */
        if (hojaDetalle == null) {
            PilaDepurada pilaDepurada = gestorProgramaDao.obtenerPilaDepuradaMesAnterior(obj.getNomina(), obj.getNominaDetalle());
            if (pilaDepurada == null) {
                /*
                if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 6)
                   System.out.println("::ANDRES25:: pilaDepurada: null");
                */
                return null;
            } else {
                String totalDiasAnteriorKey = "TOTAL_DIAS_REPORTADOS_MES_ANTERIOR#" + obj.getNominaDetalle().getNumeroIdentificacionActual() + anyoMesDetalleKey(obj);
                /*
                if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 6)
                   System.out.println("::ANDRES26:: getDiasCotSalud: " + pilaDepurada.getDiasCotSalud());
                */
                mapVariablesRegla.put(totalDiasAnteriorKey, pilaDepurada.getDiasCotSalud());
                /*
                if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 6)
                   System.out.println("::ANDRES27:: getIbcSalud: " + pilaDepurada.getIbcSalud());
                */
                return pilaDepurada.getIbcSalud();
            }
        } else {
            if(hojaDetalle.getIbcCalculadoSalud() != null){
                /*
                if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 6)
                    System.out.println("::ANDRES28:: hojaDetalle.getIbcCalculadoSalud(): " + hojaDetalle.getIbcCalculadoSalud().intValue());
                */
                return hojaDetalle.getIbcCalculadoSalud().intValue();
            }else{
                /*
                if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 6)
                   System.out.println("::ANDRES29:: hojaDetalle: " + null);
                */
                return null;
            }
            //System.out.println("::ANDRES04:: hojaDetalle: " + hojaDetalle);
            //System.out.println("::ANDRES04:: getIbcCalculadoSalud: " + hojaDetalle.getIbcCalculadoSalud());
            //System.out.println("::ANDRES04:: intValue: " + hojaDetalle.getIbcCalculadoSalud().intValue());
            //return hojaDetalle.getIbcCalculadoSalud().intValue();
        }

    }
        
        private BigInteger obtenerIBCvacaciones(GestorProgramaDao gestorProgramaDao, DatosEjecucionRegla obj) {
            //BigInteger diasSaludMesActual = new BigInteger(obj.getNominaDetalle().getDiasIncapacidadesMes().toString());   
            //if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("14965368") && obj.getNominaDetalle().getMes().intValue() == 2)
                   //System.out.println("::ANDRES91:: diasSaludMesActual: " + diasSaludMesActual); 
            try {
                BigDecimal diasVacacionesDisfrutadasMesActual = new BigDecimal(obj.getNominaDetalle().getDiasVacacionesMes().toString());   
                /*
                if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 6)
                {
                    System.out.println("::ANDRES30:: diasVacacionesDisfrutadasMesActual: " + diasVacacionesDisfrutadasMesActual);
                }
                */
                //a partir de el "número DOCUMENTO ACTUAL DEL COTIZANTE", se verifica en los meses anteriores, empezando por el más reciente 
                //al más antiguo, cuál de ellos en el campo "número DE DÍAS VACACIONES DISFRUTADAS EN EL MES" es igual a cero (0), 
                //este mes se identifica como el (mes anterior al inicio del disfrute),
                NominaDetalle nominaMesAnteriorDisfrute = gestorProgramaDao.obtenerNominaDetalleAnteriorInicioVacacionesMismoAno(obj.getNominaDetalle());
                //en caso de que hayan varios años traeria el del anterior año, se dejo asi, porque estaba
                //trayendo siempre el año anterior por ordenarlo de menr a mayor pero si hay uno en el mismo mes
                //ese seria el anterior por eso se llama la funcio que no tiene en cuenta el AÑO
                if(nominaMesAnteriorDisfrute == null){
                        //System.out.println("::ANDRES31.0:: vale NULL: " + nominaMesAnteriorDisfrute);
                   nominaMesAnteriorDisfrute = gestorProgramaDao.obtenerNominaDetalleAnteriorInicioVacaciones(obj.getNominaDetalle());
                }
                    /*
                    if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 6)
                    {
                        System.out.println("::ANDRES31.1:: getId: " + obj.getNominaDetalle().getNomina().getId());
                        System.out.println("::ANDRES31.2:: getMes: " + obj.getNominaDetalle().getMes());
                        System.out.println("::ANDRES31.3:: getAno: " + obj.getNominaDetalle().getAno());
                        System.out.println("::ANDRES31.4:: getNumeroIdentificacionActual: " + obj.getNominaDetalle().getNumeroIdentificacionActual());
                        System.out.println("::ANDRES31.5:: getTipoNumeroIdentificacionActual: " + obj.getNominaDetalle().getTipoNumeroIdentificacionActual());
                        System.out.println("::ANDRES31.6:: nominaMesAnteriorDisfrute: " + nominaMesAnteriorDisfrute);
                        System.out.println("::ANDRES31.7:: getAno: " + nominaMesAnteriorDisfrute.getAno());
                        System.out.println("::ANDRES31.8:: getMes: " + nominaMesAnteriorDisfrute.getMes());
                    }
                    */
                //En caso de no existir registro de nómina del mes anterior
                if(nominaMesAnteriorDisfrute == null){
                    PilaDepurada pilaDepurada;
                    //hacer consulta que traiga el menor mes para esta nomina, para este cotizante y menor mes
                    //donde dias vacaciones sea mayor a 0
                    NominaDetalle verificarAnioAnteriorDisfrute = gestorProgramaDao.obtenerNominaDetalleInicioVacaciones(obj.getNominaDetalle());
                    if(verificarAnioAnteriorDisfrute == null){
                       //se acude a PILA depurada del mes anterior al mes del renglon actual
                       pilaDepurada = gestorProgramaDao.obtenerPilaDepuradaMesAnterior(obj.getNomina(),obj.getNominaDetalle());
                       /*
                        if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 6)
                          System.out.println("::ANDRES32:: pilaDepurada: " + pilaDepurada); 
                        */
                    }else{
                        //se acude a PILA depurada haciendo un salto al menor mes donde dias vacaciones sea mayor a 0
                        pilaDepurada = gestorProgramaDao.obtenerPilaDepuradaMesAnterior(obj.getNomina(),verificarAnioAnteriorDisfrute);
                            /*
                             if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 6)
                                System.out.println("::ANDRES33:: pilaDepurada: " + pilaDepurada);
                            */
                    }
                    //en caso de no haber mes anterior de PILA,  colocar valor 0 (CERO).
                    if(pilaDepurada == null) {    
                           /*
                           if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 6)
                           System.out.println("::ANDRES34:: pilaDepurada: null");
                          */
                            return new BigInteger("0");
                    }else{
                        //tomando la información del IBC de salud reportado en el mes anterior dividido en los días de salud reportados 
                        //para este mismo mes y el resultado se multiplica por los días reportados de vacaciones en "número 
                        // DE DÍAS VACACIONES DISFRUTADAS EN EL MES" (actual).
                        BigDecimal ibcSaludMesAnterior = new BigDecimal(pilaDepurada.getIbcSalud().toString()).divide(new BigDecimal(pilaDepurada.getDiasCotSalud().toString()),2,RoundingMode.HALF_UP).multiply(diasVacacionesDisfrutadasMesActual);
                              /*
                                if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 6)
                                     System.out.println("::ANDRES35:: ibcSaludMesAnterior entero: " + roundValor(ibcSaludMesAnterior).toBigInteger());
                                */
                        return roundValor(ibcSaludMesAnterior).toBigInteger();
                    }
                }else{
                        //sleep(10000);
                   HojaCalculoLiquidacionDetalle liquidacionDetalleNominaMesAnteriorDisfrute = gestorProgramaDao.obtenerHojaCalculoLiqDetalleDeNominaDetalle(nominaMesAnteriorDisfrute);
                        //if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("14965368") && obj.getNominaDetalle().getMes().intValue() == 2)
                        //     System.out.println("::ANDRES97:: liquidacionDetalleNominaMesAnteriorDisfrute: " + liquidacionDetalleNominaMesAnteriorDisfrute); 
                        /*
                            if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 6)
                            {
                                System.out.println("::ANDRES50:: liquidacionDetalleNominaMesAnteriorDisfrute: " + liquidacionDetalleNominaMesAnteriorDisfrute); 
                                //System.out.println("::ANDRES50.1:: getCotizdAno: " + liquidacionDetalleNominaMesAnteriorDisfrute.getCotizdAno().intValue()); 
                                //System.out.println("::ANDRES50.2:: getCotizdMes: " + liquidacionDetalleNominaMesAnteriorDisfrute.getCotizdMes().intValue()); 
                            }
                            */
                        //se toma el valor del “TOTAL IBC CALCULADO SALUD” del (mes anterior al inicio del disfrute) y se divide entre el 
                        //“TOTAL DÍAS REPORTADOS EN EL MES” de (mes anterior al inicio del disfrute)  
                        //y el resultado se multiplica por los días reportados de vacaciones en "número DE DÍAS VACACIONES DISFRUTADAS EN EL MES" (actual).
                    BigDecimal totalIbcCalculadoSaludMesAnteriorDisfrute;
                    BigDecimal totalDiasReportadosMesAnteriorDisfrute;
                    if(obj.getNominaDetalle().getDobleLineaAnterior() == null) {
                            /*
                            if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 6)
                            {
                             System.out.println("::ANDRES70:: id: " + liquidacionDetalleNominaMesAnteriorDisfrute.getId());
                             System.out.println("::ANDRES71:: getIbcCalculadoSalud: " + liquidacionDetalleNominaMesAnteriorDisfrute.getIbcCalculadoSalud());
                            }
                            */
                            totalIbcCalculadoSaludMesAnteriorDisfrute =  new BigDecimal(liquidacionDetalleNominaMesAnteriorDisfrute.getIbcCalculadoSalud());
                            totalDiasReportadosMesAnteriorDisfrute = new BigDecimal(nominaMesAnteriorDisfrute.getTotalDiasReportadosMes().toString());
                            /*
                            if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 6)
                            {
                             System.out.println("::ANDRES60:: totalIbcCalculadoSaludMesAnteriorDisfrute: " + totalIbcCalculadoSaludMesAnteriorDisfrute);
                             System.out.println("::ANDRES61:: totalDiasReportadosMesAnteriorDisfrute: " + totalDiasReportadosMesAnteriorDisfrute);
                            }
                            */
                    }else{
                        totalIbcCalculadoSaludMesAnteriorDisfrute =  gestorProgramaDao.sumarIbcCalculadoSaludMesDobleLineaAnterior(obj.getNominaDetalle()); //sumar IBC_CALCULADO_SALUD - HojaCalculoLiquidacionDetalle
                        totalDiasReportadosMesAnteriorDisfrute =  gestorProgramaDao.sumarTotalDiasReportadosMesDobleLineaAnterior(obj.getNominaDetalle());  //sumar TOTAL_DIAS_REPORTADOS_MES - nominadetalle
                            /*
                            if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 6)
                            {
                             System.out.println("::ANDRES62:: totalIbcCalculadoSaludMesAnteriorDisfrute: " + totalIbcCalculadoSaludMesAnteriorDisfrute);
                             System.out.println("::ANDRES63:: totalDiasReportadosMesAnteriorDisfrute: " + totalDiasReportadosMesAnteriorDisfrute);
                            }
                            */
                    }
                        /*
                        if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 6)
                        {
                         System.out.println("::ANDRES36:: totalIbcCalculadoSaludMesAnteriorDisfrute: " + totalIbcCalculadoSaludMesAnteriorDisfrute);  
                         System.out.println("::ANDRES37:: totalDiasReportadosMesAnteriorDisfrute: " + totalDiasReportadosMesAnteriorDisfrute); 
                        }
                        */
                    BigDecimal ibcVacaciones = (totalIbcCalculadoSaludMesAnteriorDisfrute.divide(totalDiasReportadosMesAnteriorDisfrute,2,RoundingMode.HALF_UP)).multiply(diasVacacionesDisfrutadasMesActual);
                        /*
                        if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 6)
                           System.out.println("::ANDRES38:: ibcVacaciones: " + roundValor(ibcVacaciones).toBigInteger()); 
                        */
                    return roundValor(ibcVacaciones).toBigInteger();
                }
           }catch(Exception e){               
               /*
               if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 6)
               {    
                    System.out.println("::ANDRES39:: obtenerIBCvacaciones Se devuelve 0. Exception " + e.getMessage());
                    e.printStackTrace();  
               }
               */
               return BigInteger.ZERO;
           }
	}
    
    private Integer obtenerTotalDiasReportadosMesAnterior(GestorProgramaDao gestorProgramaDao, DatosEjecucionRegla obj) {

        NominaDetalle nomDet = gestorProgramaDao.nominaDetalleAnteriorByNominaDetalleMesAnterior(obj);
        //System.out.println("::ANDRES81:: obj.getNominaDetalle: " + obj.getNominaDetalle().toString());
        //System.out.println("::ANDRES81:: nomDet: " + nomDet.toString());
        if (nomDet == null) {
            //System.out.println("::ANDRES81:: getTotalDiasReportadosMes: " + "0" + " ::MES:: " + obj.getNominaDetalle().getMes() + " idNominaDetalle: " + nomDet.getId());
            /*
            if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 6)
                System.out.println("::ANDRES40:: getTotalDiasReportadosMes: " + "0");
            */
            return new Integer("0");
        } else {
            //System.out.println("::ANDRES81:: getTotalDiasReportadosMes: " + nomDet.getTotalDiasReportadosMes() + " ::MES:: " + obj.getNominaDetalle().getMes() + " idNominaDetalle: " + nomDet.getId());
            /*
            if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 6)
                System.out.println("::ANDRES41:: getTotalDiasReportadosMes: " + nomDet.getTotalDiasReportadosMes());
            */
            return nomDet.getTotalDiasReportadosMes();
        }
    }

    /**
     * Se hace logica en programacion para aquellas reglas que no tienen formula
     *
     * @param errorTipo
     * @param gestorProgramaDao
     * @param obj
     * @param infoNegocio
     * @param pilaDepurada
     */
    @Override
    public void procesarReglasNoFormula(List<ErrorTipo> errorTipo, GestorProgramaDao gestorProgramaDao, DatosEjecucionRegla obj, Map<String, Object> infoNegocio, PilaDepurada pilaDepurada) {

            // Mayo 26.2016 - Llamado a métodos para manejo de objetos en memoria Caché
            CacheService cacheService = new CacheService();
            cacheService.createInstance();
            cacheService.putAll(CacheService.REGION_COBPARAMGENERAL, gestorProgramaDao.findAll(CobParamGeneral.class));
            Map<String, String> mapMallaval = gestorProgramaDao.obtenerMallaVal(obj.getNominaDetalle());
            BigDecimal tipoIdentificacionAportante = gestorProgramaDao.tipoIdentificacionAportante(obj.getNominaDetalle());
            //cacheService.putAll(CacheService.REGION_COBFLEX, gestorProgramaDao.findAll(CobFlex.class));
            //cacheService.putAll(CacheService.REGION_APORTANTELIQ, jpaEntityDao.findAll(Tipoexpediente.class));
            //cacheService.putAll(CacheService.REGION_COBSBSIS,  gestorProgramaDao.findAll(CobSbsis.class));
            
            // Se busca información de la liquidación anterior. Requerimiento del 26 de Mayo, solicitado por correo Fabio López
            //HojaCalculoLiquidacionDetalle hojaDetalle = gestorProgramaDao.obtenerHojaCalculoLiqDetalleMesAnterior(obj.getNomina(), obj.getNominaDetalle(), obj.getCotizante());
            // Regla #1 <IBC PERMISOS REMUNERADOS>
            // Regla #2 <IBC SUSPENSIONES O PERMISOS NO REMUNERADOS>
            // Regla #3 <IBC VACACIONES>
            // Regla #4 <IBC HUELGA>
            
            // Regla #5 <DIAS_COT_PENSION>
            int diasCotPension = obj.getNominaDetalle().getDiasTrabajadosMes() + obj.getNominaDetalle().getDiasIncapacidadesMes() + obj.getNominaDetalle().getDiasLicenciaRemuneradasMes()
                    + obj.getNominaDetalle().getDiasLicenciaMaternidadPaternidadMes() + obj.getNominaDetalle().getDiasVacacionesMes() + obj.getNominaDetalle().getDiasHuelgaLegalMes();
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#DIAS_COT_PENSION" + anyoMesDetalleKey(obj), diasCotPension);
            
            // Regla #6 <DIAS_COT_SALUD> 
            int diasCotSalud = diasCotPension + obj.getNominaDetalle().getDiasSuspensionMes();
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual()  + "#DIAS_COT_SALUD" + anyoMesDetalleKey(obj), diasCotSalud);
            
            // Regla #7 <DIAS_COT_RPROF>
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual()  + "#DIAS_COT_RPROF" + anyoMesDetalleKey(obj), obj.getNominaDetalle().getDiasTrabajadosMes());
            
            // Regla #8 <DIAS_COT_CCF>
            // 04.Oct.2019. WR (Req.Trabajadores Independientes)
            int diasCotCCF=0;
            if(!"2".equals(obj.getNominaDetalle().getIdaportante().getTipoAportante())){
                diasCotCCF = obj.getNominaDetalle().getDiasTrabajadosMes() + obj.getNominaDetalle().getDiasVacacionesMes();
            }
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#DIAS_COT_CCF" + anyoMesDetalleKey(obj), diasCotCCF);
            // Regla #9 <TOTAL PAGOS NO SALARIALES>
            BigDecimal sumValorPagoNoSalarial = gestorProgramaDao.sumaValorLiqConceptoContablePagoNoSalarial(obj.getNomina(), obj.getNominaDetalle());
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#PAGO_NO_SALARIAL" + anyoMesDetalleKey(obj), sumValorPagoNoSalarial);
            // Regla #10 <TOTAL REMUNERADO>
            // 04.Oct.2019. WR (Req.Trabajadores Independientes)
            BigDecimal sumValorTotalRemunerado =  BigDecimal.ZERO;
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual()  + "#TOTAL_REMUNERADO" + anyoMesDetalleKey(obj), BigDecimal.ZERO);                            
            if(!"2".equals(obj.getNominaDetalle().getIdaportante().getTipoAportante())){
                if(!obj.getNominaDetalle().getTipoCotizante().equals("31")){
                    sumValorTotalRemunerado = gestorProgramaDao.sumaValorLiqConceptoContableTotalRemunerado(obj.getNomina(), obj.getNominaDetalle());
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual()  + "#TOTAL_REMUNERADO" + anyoMesDetalleKey(obj), sumValorTotalRemunerado);
                }else{
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual()  + "#TOTAL_REMUNERADO" + anyoMesDetalleKey(obj), BigDecimal.ZERO);
                }
            }
            
            // Regla # <TOTAL DEVENGADO>
            // 04.Oct.2019. WR (Req.Trabajadores Independientes)
            BigDecimal sumValorTotalDevengado =  BigDecimal.ZERO;
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TOTAL_DEVENGADO" + anyoMesDetalleKey(obj), BigDecimal.ZERO);
            if(!"2".equals(obj.getNominaDetalle().getIdaportante().getTipoAportante())){
                if(!obj.getNominaDetalle().getTipoCotizante().equals("31")) {
                  sumValorTotalDevengado = gestorProgramaDao.sumaValorLiqConceptoContableTotalDevengado(obj.getNomina(), obj.getNominaDetalle());
                  infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TOTAL_DEVENGADO" + anyoMesDetalleKey(obj), sumValorTotalDevengado);
                }                
            }
            // Regla #11 <PORCENTAJE PAGOS NO SALARIALES>
            // 04.Oct.2019. WR (Req.Trabajadores Independientes)
            BigDecimal porPagoNoSalarial = BigDecimal.ZERO;
            if(!"2".equals(obj.getNominaDetalle().getIdaportante().getTipoAportante())){
               //andres se rompia por valor a 0
               if (sumValorTotalRemunerado.compareTo(BigDecimal.ZERO) != 0) {
                   porPagoNoSalarial = sumValorPagoNoSalarial.divide(sumValorTotalRemunerado, 2, RoundingMode.CEILING);
               }
               porPagoNoSalarial = porPagoNoSalarial.multiply(new BigDecimal("100"));
            }
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#POR_PAGO_NO_SALARIAL" + anyoMesDetalleKey(obj), porPagoNoSalarial);
            // Regla #12 <EXCEDENTE LIMITE DE PAGO NO SALARIAL>
            // 04.Oct.2019. WR (Req.Trabajadores Independientes)
            BigDecimal rst = BigDecimal.ZERO;
            BigDecimal resta = BigDecimal.ZERO;
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#EXC_LIM_PAGO_NO_SALARIAL" + anyoMesDetalleKey(obj), new BigDecimal("0"));
            if(!"2".equals(obj.getNominaDetalle().getIdaportante().getTipoAportante())){
                CobFlex cobFlex = gestorProgramaDao.obtenerCobFlexByFecha(obj.getNominaDetalle());
                BigDecimal por100Dec = new BigDecimal("0");
                if (cobFlex != null) {
                   por100Dec = new BigDecimal(cobFlex.getPorcentajeFlex());
                }
                por100Dec = por100Dec.divide(new BigDecimal("100"));
                rst = mulValorReglas(por100Dec, infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TOTAL_REMUNERADO" + anyoMesDetalleKey(obj)));
                BigDecimal pagoNoSal = convertValorRegla(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#POR_PAGO_NO_SALARIAL" + anyoMesDetalleKey(obj)));
                if (cobFlex != null && pagoNoSal.doubleValue() > cobFlex.getPorcentajeFlex().doubleValue()) {
                   resta = minusValorReglas(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual()  + "#PAGO_NO_SALARIAL" + anyoMesDetalleKey(obj)), rst);
                   infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#EXC_LIM_PAGO_NO_SALARIAL" + anyoMesDetalleKey(obj), resta);
                } else {
                   infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#EXC_LIM_PAGO_NO_SALARIAL" + anyoMesDetalleKey(obj), new BigDecimal("0"));
                }                                
            }
            
        // Regla #13 <CODIGO ADMINISTRADORA SALUD> :: Para el caso de Trabajadores Independientes
        // Octubre 10.2019 WROJAS
        if ("2".equals(obj.getNominaDetalle().getIdaportante().getTipoAportante()) && ("RÉGIMEN ESPECIAL FFMM".equals(obj.getNominaDetalle().getCondEspTrab()) || "RÉGIMEN ESPECIAL MAGISTERIO".equals(obj.getNominaDetalle().getCondEspTrab()) || "RÉGIMEN ESPECIAL ECOPETROL".equals(obj.getNominaDetalle().getCondEspTrab()) || "RÉGIMEN ESPECIAL UNIVERSIDAD ESTATAL".equals(obj.getNominaDetalle().getCondEspTrab()))) {
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COD_ADM_SALUD" + anyoMesDetalleKey(obj), "MIN002");
            // EN este caso, también se coloca fijo el nombre corto.
            // Regla #14 <NOMBRE CORTO ADMINISTRADORA SALUD>
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#NOM_ADM_SALUD" + anyoMesDetalleKey(obj), "Fosyga Regimen de Excepción");
        } else {
            if (pilaDepurada != null) {
                // Regla #13 <CODIGO ADMINISTRADORA SALUD>
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COD_ADM_SALUD" + anyoMesDetalleKey(obj), pilaDepurada.getCodigoEPS());
                // Regla #14 <NOMBRE CORTO ADMINISTRADORA SALUD>
                if (pilaDepurada.getCodigoEPS() != null) {
                    String nomAdmSalud = LST_ADMINISTRADORA_PILA.get(pilaDepurada.getCodigoEPS());
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#NOM_ADM_SALUD" + anyoMesDetalleKey(obj), nomAdmSalud);
                }
            } else {  // Entra al <else> cuando Pila depurada es NULL
                // Regla #13 <CODIGO ADMINISTRADORA SALUD> si piladepurada=null 
                HojaCalculoLiquidacionDetalle liquidacionDetalleMesAnterior = gestorProgramaDao.obtenerHojaCalculoLiqDetalleMesAnterior(obj.getNomina(), obj.getNominaDetalle(), infoNegocio.get("IDHOJACALCULOLIQUIDACION").toString());
                if (liquidacionDetalleMesAnterior != null && StringUtils.isNotBlank(liquidacionDetalleMesAnterior.getCodAdmSalud())) {
                    //System.out.println("::ANDRES1:: SALUD " + liquidacionDetalleMesAnterior.getCodAdmSalud());
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COD_ADM_SALUD" + anyoMesDetalleKey(obj), liquidacionDetalleMesAnterior.getCodAdmSalud());
                    // Regla #14 <NOMBRE CORTO ADMINISTRADORA SALUD> si piladepurada=null 
                    String nomAdmSalud = LST_ADMINISTRADORA_PILA.get(liquidacionDetalleMesAnterior.getCodAdmSalud());
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#NOM_ADM_SALUD" + anyoMesDetalleKey(obj), nomAdmSalud);
                } else {
                    //se debe realizar la búsqueda con el  (número DOCUMENTO CON EL QUE REALIZO APORTES DEL COTIZANTE)en el PILA DEPURADA
                    //del aportante en el mes inmediatamente anterior se acude a PILA depurada del mes anterior al mes del renglon actual
                    PilaDepurada pilaDepuradaMesAnterior = gestorProgramaDao.obtenerPilaDepuradaMesAnterior(obj.getNomina(), obj.getNominaDetalle());
                    //System.out.println("::ANDRES85:: getNumeroIdentificacion: " + obj.getNominaDetalle().getNumeroIdentificacionActual());
                    if (pilaDepuradaMesAnterior != null) {
                        //System.out.println("::ANDRES2:: SALUD pilaDepuradaMesAnterior: " + pilaDepuradaMesAnterior.getId());
                        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COD_ADM_SALUD" + anyoMesDetalleKey(obj), pilaDepuradaMesAnterior.getCodigoEPS());
                        String nomAdmSalud = LST_ADMINISTRADORA_PILA.get(pilaDepuradaMesAnterior.getCodigoEPS());
                        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#NOM_ADM_SALUD" + anyoMesDetalleKey(obj), nomAdmSalud);
                    } else {
                        //En caso de no existir registros del mes fiscalizado en el PILA DEPURADA
                        //se debe traer la información registrada en la columna " OBSERVACIONES APORTANTE SALUD"
                        String codigoObservacionSalud = gestorProgramaDao.observacionSalud(obj.getNominaDetalle());
                        //System.out.println("::ANDRES3:: SALUD codigoObservacionSalud: " + codigoObservacionSalud);
                        if (codigoObservacionSalud != null) {
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COD_ADM_SALUD" + anyoMesDetalleKey(obj), codigoObservacionSalud);
                            String nomAdmSalud = LST_ADMINISTRADORA_PILA.get(codigoObservacionSalud);
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#NOM_ADM_SALUD" + anyoMesDetalleKey(obj), nomAdmSalud);
                        }
                        //En caso de no existir información en PILA DEPURADA ni tampoco en la columna "OBSERVACIONES APORTANTE SALUD" se deja vacío.
                    }
                }  // FIN EVALUACIÓN REGLA #13                                  
            }
        }  // FIN REGLA <CODIGO ADMINISTRADORA SALUD> para el caso de trabajadores INdependientes
            //WROJAS - AJUSTE NOVIEMBRE 22.2019
            if (pilaDepurada !=null){
                // Regla #20 <DIAS COTIZADOS PILA SALUD>
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual()  + "#DIAS_COTIZ_PILA_SALUD" + anyoMesDetalleKey(obj), pilaDepurada.getDiasCotSalud());
                // Regla #21 <IBC PILA SALUD>
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_PILA_SALUD" + anyoMesDetalleKey(obj), pilaDepurada.getIbcSalud());
                // Regla #22 <TARIFA PILA SALUD>
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual()  + "#TARIFA_PILA_SALUD" + anyoMesDetalleKey(obj), pilaDepurada.getTarifaSalud());
                // Regla #23 <COTIZACION PAGADA PILA SALUD>
                if(StringUtils.isNotBlank(obj.getNominaDetalle().getCargueManualPilaSalud().toString()) &&  convertValorRegla(obj.getNominaDetalle().getCargueManualPilaSalud().toString()).intValue() > 0){
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual()  + "#COTIZ_PAGADA_PILA_SALUD" + anyoMesDetalleKey(obj), obj.getNominaDetalle().getCargueManualPilaSalud().toString());
                }else{
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual()  + "#COTIZ_PAGADA_PILA_SALUD" + anyoMesDetalleKey(obj), pilaDepurada.getCotObligatoriaSalud());
                }                
                // Regla #28 <CODIGO ADMINISTRADORA PENSION>
                if (StringUtils.isNotBlank(pilaDepurada.getCodigoAFP())) {
                        //System.out.println("::ANDRES1:: [" + obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COD_ADM_PENSION ["  + pilaDepurada.getCodigoAFP() + "] piladepurada " + pilaDepurada.getId());
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual()  + "#COD_ADM_PENSION" + anyoMesDetalleKey(obj), pilaDepurada.getCodigoAFP());
                } else {
                    String codObsPension = gestorProgramaDao.observacionPension(obj.getNominaDetalle());
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual()  + "#COD_ADM_PENSION" + anyoMesDetalleKey(obj), codObsPension);
                }                
                // Regla #29 <NOMBRE CORTO ADMINISTRADORA PENSION>
                if (!StringUtils.isBlank(pilaDepurada.getCodigoAFP())) {
                    String nomAdmPension = LST_ADMINISTRADORA_PILA.get(pilaDepurada.getCodigoAFP());
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual()  + "#NOM_ADM_PENSION" + anyoMesDetalleKey(obj), nomAdmPension);
                } else {
                    String codObsPension = gestorProgramaDao.observacionPension(obj.getNominaDetalle());
                    String nomAdmPension = LST_ADMINISTRADORA_PILA.get(codObsPension);
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual()  + "#NOM_ADM_PENSION" + anyoMesDetalleKey(obj), nomAdmPension);
                }                
                // Regla #34 <DIAS COTIZADOS PILA PENSION>
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual()  + "#DIAS_COTIZ_PILA_PENSION" + anyoMesDetalleKey(obj), pilaDepurada.getDiasCotPension());
                // Regla #35 <IBC PILA PENSION>
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual()  + "#IBC_PILA_PENSION" + anyoMesDetalleKey(obj), pilaDepurada.getIbcPension());                
                // Regla #36 <TARIFA PILA PENSION>
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_PILA_PENSION" + anyoMesDetalleKey(obj), pilaDepurada.getTarifaPension());                    
                // Regla #37 <COTIZACION PAGADA PILA PENSION>
                if(StringUtils.isNotBlank(obj.getNominaDetalle().getCargueManualPilaPension().toString()) &&  convertValorRegla(obj.getNominaDetalle().getCargueManualPilaPension().toString()).intValue() > 0){
                    //System.out.println("::ANDRES4:: manual getCargueManualPilaPension: " + obj.getNominaDetalle().getCargueManualPilaPension().toString());
                    //System.out.println("::ANDRES4:: manual anyoMesDetalleKey: " + anyoMesDetalleKey(obj));
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual()  + "#COT_PAGADA_PILA_PENSION" + anyoMesDetalleKey(obj), obj.getNominaDetalle().getCargueManualPilaPension().toString());
                }else{
                        //System.out.println("::ANDRES5:: manual getCargueManualPilaPension: " + obj.getNominaDetalle().getCargueManualPilaPension().toString());
                        //System.out.println("::ANDRES5:: manual anyoMesDetalleKey: " + anyoMesDetalleKey(obj));
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COT_PAGADA_PILA_PENSION" + anyoMesDetalleKey(obj), pilaDepurada.getAporteCotObligatoriaPension());
                }                    
                // Regla #46 <COTIZACION PAGADA PILA FSP SUBCUENTA DE SOLIDARIDAD>
                if(StringUtils.isNotBlank(obj.getNominaDetalle().getCargueManualPilaFspSolid().toString()) && convertValorRegla(obj.getNominaDetalle().getCargueManualPilaFspSolid().toString()).intValue() > 0)
                   infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_PAG_PILA_FSP_SUB_SOLIDAR" + anyoMesDetalleKey(obj), obj.getNominaDetalle().getCargueManualPilaFspSolid().toString());
                else
                   infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_PAG_PILA_FSP_SUB_SOLIDAR" + anyoMesDetalleKey(obj), pilaDepurada.getAporteFsolidPensionalSolidaridad());

                // Regla #47 <COTIZACION PAGADA PILA FSP SUBCUENTA DE SUBSISTENCIA>
                if(StringUtils.isNotBlank(obj.getNominaDetalle().getCargueManualPilaFspSubsis().toString()) && convertValorRegla(obj.getNominaDetalle().getCargueManualPilaFspSubsis().toString()).intValue() > 0)
                   infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual()  + "#COTIZ_PAG_PILA_FSP_SUB_SUBSIS" + anyoMesDetalleKey(obj), obj.getNominaDetalle().getCargueManualPilaFspSubsis().toString());
                else
                   infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual()  + "#COTIZ_PAG_PILA_FSP_SUB_SUBSIS" + anyoMesDetalleKey(obj), pilaDepurada.getAporteFsolidPensionalSubsistencia());
                
                // Regla #62 <CODIGO ADMINISTRADORA ARL>
                if (pilaDepurada.getCodigoArp() != null) {
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual()  + "#COD_ADM_ARL" + anyoMesDetalleKey(obj), pilaDepurada.getCodigoArp());
                    // Regla #63 <NOMBRE CORTO ADMINISTRADORA ARL>
                    String nomAdmArl = LST_ADMINISTRADORA_PILA.get(pilaDepurada.getCodigoArp());
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual()  + "#NOM_ADM_ARL" + anyoMesDetalleKey(obj), nomAdmArl);
                }                    
            }else{ // Ingresa al ELSE cuando PilaDepurada es NULL
                // Regla #37 <COTIZACION PAGADA PILA PENSION>
                if(StringUtils.isNotBlank(obj.getNominaDetalle().getCargueManualPilaPension().toString()) && convertValorRegla(obj.getNominaDetalle().getCargueManualPilaPension().toString()).intValue() > 0)
                   infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual()  + "#COT_PAGADA_PILA_PENSION" + anyoMesDetalleKey(obj), obj.getNominaDetalle().getCargueManualPilaPension().toString());
                else
                   infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual()  + "#COT_PAGADA_PILA_PENSION" + anyoMesDetalleKey(obj), new BigDecimal("0"));
               
                // Regla #23 <COTIZACION PAGADA PILA SALUD>
                if(StringUtils.isNotBlank(obj.getNominaDetalle().getCargueManualPilaSalud().toString()) && convertValorRegla(obj.getNominaDetalle().getCargueManualPilaSalud().toString()).intValue() > 0)
                   infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_PAGADA_PILA_SALUD" + anyoMesDetalleKey(obj), obj.getNominaDetalle().getCargueManualPilaSalud().toString());
                else
                   infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_PAGADA_PILA_SALUD" + anyoMesDetalleKey(obj), new BigDecimal("0"));                

                // Regla #62 <CODIGO ADMINISTRADORA ARL> si piladepurada=null 
                HojaCalculoLiquidacionDetalle liquidacionDetalleMesAnterior = gestorProgramaDao.obtenerHojaCalculoLiqDetalleMesAnterior(obj.getNomina(), obj.getNominaDetalle(), infoNegocio.get("IDHOJACALCULOLIQUIDACION").toString());                    
                if (liquidacionDetalleMesAnterior != null && StringUtils.isNotBlank(liquidacionDetalleMesAnterior.getCodAdmArl())) {
                    //System.out.println("::ANDRES1:: ARL " + liquidacionDetalleMesAnterior.getCodAdmSalud());
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COD_ADM_ARL" + anyoMesDetalleKey(obj), liquidacionDetalleMesAnterior.getCodAdmArl());
                    // Regla #63 <NOMBRE CORTO ADMINISTRADORA ARL>  si piladepurada=null 
                    String nomAdmArl = LST_ADMINISTRADORA_PILA.get(liquidacionDetalleMesAnterior.getCodAdmArl());
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#NOM_ADM_ARL" + anyoMesDetalleKey(obj), nomAdmArl);
                }else{
                   //se debe realizar la búsqueda con el número DOCUMENTO CON EL QUE REALIZO APORTES DEL COTIZANTE en la PILA DEPURADA 
                   //del aportante en el mes inmediatamente anterior se acude a PILA depurada del mes anterior al mes del renglon actual
                   PilaDepurada pilaDepuradaMesAnterior = gestorProgramaDao.obtenerPilaDepuradaMesAnterior(obj.getNomina(),obj.getNominaDetalle());
                   //System.out.println("::ANDRES86:: getNumeroIdentificacion: " + obj.getNominaDetalle().getNumeroIdentificacionActual());
                    if(pilaDepuradaMesAnterior != null){
                      //System.out.println("::ANDRES2:: ARL pilaDepuradaMesAnterior: " + pilaDepuradaMesAnterior.getId());
                      infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual()  + "#COD_ADM_ARL" + anyoMesDetalleKey(obj), pilaDepuradaMesAnterior.getCodigoArp());
                      String nomAdmArl = LST_ADMINISTRADORA_PILA.get(pilaDepuradaMesAnterior.getCodigoArp());
                      infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#NOM_ADM_ARL" + anyoMesDetalleKey(obj), nomAdmArl);
                    }else{
                        //En caso de no existir registros del mes fiscalizado en el PILA DEPURADA se debe traer la información registrada en la columna
                        //"OBSERVACIONES APORTANTE ARL"
                        String codigoObservacionArl = gestorProgramaDao.observacionArl(obj.getNominaDetalle());
                        //System.out.println("::ANDRES3:: ARL codigoObservacionSalud: " + codigoObservacionArl);
                        if (codigoObservacionArl != null){
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual()  + "#COD_ADM_ARL" + anyoMesDetalleKey(obj), codigoObservacionArl);
                            String nomAdmArl = LST_ADMINISTRADORA_PILA.get(codigoObservacionArl);
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual()  + "#NOM_ADM_SALUD" + anyoMesDetalleKey(obj), nomAdmArl);
                        }
                            //En caso de no existir información en PILA DEPURADA ni tampoco en la columna "OBSERVACIONES APORTANTE ARL" se deja vacío.
                    }
                } // FIN EVALUACIÓN REGLA # 62 CUANDO PILA DEPURADA ES NULL
                //Regla #75 <CODIGO ADMINISTRADORA CCF>  si piladepurada=null 
                if (liquidacionDetalleMesAnterior != null && StringUtils.isNotBlank(liquidacionDetalleMesAnterior.getCodAdmCcf())) {
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual()  + "#COD_ADM_CCF" + anyoMesDetalleKey(obj), liquidacionDetalleMesAnterior.getCodAdmCcf());
                        //System.out.println("::ANDRES10:: liquidacionDetalleMesAnterior: " + liquidacionDetalleMesAnterior.getId() + " MES: " + anyoMesDetalleKey(obj) + " CCF: " + liquidacionDetalleMesAnterior.getCodAdmCcf());
                //Regla #76 <NOMBRE CORTO ADMINISTRADORA CCF>  si piladepurada=null 
                    String nomAdmCcf = LST_ADMINISTRADORA_PILA.get(liquidacionDetalleMesAnterior.getCodAdmCcf());
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual()  + "#NOM_ADM_CCF" + anyoMesDetalleKey(obj), nomAdmCcf);
                }else{
                        //se debe realizar la búsqueda con el  (número DOCUMENTO CON EL QUE REALIZO APORTES DEL COTIZANTE)en el PILA DEPURADA del aportante
                        //en el mes inmediatamente anterior se acude a PILA depurada del mes anterior al mes del renglon actual
                    PilaDepurada pilaDepuradaMesAnterior = gestorProgramaDao.obtenerPilaDepuradaMesAnterior(obj.getNomina(),obj.getNominaDetalle());
                    if(pilaDepuradaMesAnterior != null){
                       infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual()  + "#COD_ADM_CCF" + anyoMesDetalleKey(obj), pilaDepuradaMesAnterior.getCodigoCCF());
                            //System.out.println("::ANDRES11:: obj.getNominaDetalle(): " + obj.getNominaDetalle().getId() + " MES: " + anyoMesDetalleKey(obj) + " CCF: " + pilaDepuradaMesAnterior.getCodigoCCF());
                       String nomAdmCcf = LST_ADMINISTRADORA_PILA.get(pilaDepuradaMesAnterior.getCodigoCCF());
                       infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual()  + "#NOM_ADM_CCF" + anyoMesDetalleKey(obj), nomAdmCcf);
                    }else{
                        //En caso de no existir registros del mes fiscalizado en el PILA DEPURADA se debe traer la información registrada en la columna
                        //"OBSERVACIONES APORTANTE CCF"
                        String codigoObservacionCCF = gestorProgramaDao.observacionCCF(obj.getNominaDetalle());
                        if (codigoObservacionCCF != null){
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COD_ADM_CCF" + anyoMesDetalleKey(obj), codigoObservacionCCF);
                                //System.out.println("::ANDRES12:: getNominaDetalle: " + obj.getNominaDetalle().getId() + " MES: " + anyoMesDetalleKey(obj) + " CCF: " + codigoObservacionCCF);
                            String nomAdmCcf = LST_ADMINISTRADORA_PILA.get(codigoObservacionCCF);
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#NOM_ADM_CCF" + anyoMesDetalleKey(obj), nomAdmCcf);
                        }
                            //En caso de no existir información en PILA DEPURADA ni tampoco en la columna "OBSERVACIONES APORTANTE CCF" se deja vacío.
                    }
                } // FIN REGLA # 75 CUANDO PILA DEPURADA ES NULL
                // Regla #28 <CODIGO ADMINISTRADORA PENSION> si piladepurada=null
                String codObsPension = gestorProgramaDao.observacionPension(obj.getNominaDetalle());
                if (StringUtils.isNotBlank(codObsPension)) {
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COD_ADM_PENSION" + anyoMesDetalleKey(obj), codObsPension);
                // Regla #29 <NOMBRE CORTO ADMINISTRADORA PENSION> si piladepurada=null 
                    String nomAdmCcf = LST_ADMINISTRADORA_PILA.get(codObsPension);
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#NOM_ADM_PENSION" + anyoMesDetalleKey(obj), nomAdmCcf);
                }                
            } // TERMINA VALIDACION para <PilaDepurada>
            
        // Regla #15 <IBC PAGOS EN NOMINA SALUD>
        BigDecimal sumNOTpIncapacidad = gestorProgramaDao.sumaValorLiqConceptoContableIbcPagosNomSaludNoTpIncapacidad(obj.getNomina(), obj.getNominaDetalle(), "1");
        sumNOTpIncapacidad = sumValorReglas(sumNOTpIncapacidad, infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#EXC_LIM_PAGO_NO_SALARIAL"
                + anyoMesDetalleKey(obj)));
        BigDecimal sumTpIncapacidad = gestorProgramaDao.sumaValorLiqConceptoContableIbcPagosNomSalud(obj.getNomina(), obj.getNominaDetalle());
        if (StringUtils.containsIgnoreCase("X", obj.getNominaDetalle().getSalarioIntegral())) {
            BigDecimal por70 = sumNOTpIncapacidad.multiply(new BigDecimal("70"));// FIXME valor quemado
            sumNOTpIncapacidad = por70.divide(new BigDecimal("100"), 2, RoundingMode.HALF_UP); // Linea nueva
        }
        sumNOTpIncapacidad = sumNOTpIncapacidad.add(sumTpIncapacidad);
        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_PAGOS_NOM_SALUD" + anyoMesDetalleKey(obj), roundValor(sumNOTpIncapacidad));

        // Regla #16 <TOTAL IBC CALCULADO SALUD>
        String tieneSalud = gestorProgramaDao.tieneSaludCotizante(obj.getNominaDetalle());
        if (null == tieneSalud) {
            try {//System.out.println("::ERROR ANDRES44:: No encontro codigo tipo cotizante." + tieneSalud)
                ErrorTipo errorObj = new ErrorTipo();
                errorObj.setCodError("IBC_CALCULADO_SALUD");
                errorObj.setValDescError("EXCEPTION ERROR procesarReglasNoFormula: No encontro tipo cotizante en la malla del liquidador Nominadetalle = " + obj.getNominaDetalle().getId());
                errorTipo.add(errorObj);
                throw new Exception("EXCEPTION ERROR procesarReglasNoFormula: No encontro tipo cotizante en la malla del liquidador Nominadetalle = " + obj.getNominaDetalle().getId());
            } catch (Exception ex) {
                Logger.getLogger(NominaModoEjecucion.class.getName()).log(Level.SEVERE, null, ex);
            }
        } else {
            switch (tieneSalud) {
                case "NO":
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_SALUD" + anyoMesDetalleKey(obj), new BigDecimal("0"));
                    break;
                default:
                    // WROJAS - Oct.04.2019 RQ: Trabajadores Independientes.
                    cobParamGeneral = (CobParamGeneral) cacheService.get(CacheService.REGION_COBPARAMGENERAL, "SMMLV" + obj.getNominaDetalle().getAno().toString());
                    BigDecimal rstSmml = new BigDecimal("0");
                    if (cobParamGeneral != null && cobParamGeneral.getValor() != 0) {
                        rstSmml = new BigDecimal(cobParamGeneral.getValor().toString());
                    }
                    if ("2".equals(obj.getNominaDetalle().getIdaportante().getTipoAportante())) {
                        // Se busca el total del ingreso bruto - Tipo de pago
                        boolean seCumpleIBC = false;
                        BigDecimal sumIngresoBruto = gestorProgramaDao.sumaValorLiqConceptoContableTotalIBCCalculadoSaludIngresoBruto(obj.getNomina(), obj.getNominaDetalle());
                        SimpleDateFormat formateador = new SimpleDateFormat("dd/MM/yyyy");
                        try {
                            Date fechaControlTrabIndep1 = formateador.parse("30/06/2015");
                            Date fechaControlTrabIndep2 = formateador.parse("30/06/2015");
                            Date fechaRegistroTrabIndep = formateador.parse("01/" + obj.getNominaDetalle().getMes() + "/" + obj.getNominaDetalle().getAno());
                            if (fechaRegistroTrabIndep.before(fechaControlTrabIndep1) && sumIngresoBruto.doubleValue() <= 0) {
                                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_SALUD" + anyoMesDetalleKey(obj), new BigDecimal("0"));
                                seCumpleIBC = true;
                            }
                            if (fechaRegistroTrabIndep.after(fechaControlTrabIndep2) && sumIngresoBruto.doubleValue() < rstSmml.doubleValue()) {
                                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_SALUD" + anyoMesDetalleKey(obj), new BigDecimal("0"));
                                seCumpleIBC = true;
                            }
                        } catch (ParseException ex) {
                            Logger.getLogger(NominaModoEjecucion.class.getName()).log(Level.SEVERE, null, ex);
                        }
                        // Código que se repite Caso INDEPENDIENTES solo para los topes

                        if (!seCumpleIBC) { // Si ninguna de las dos condiciones anteriores se cumple, se verifican los topes
                            if (obj.getNominaDetalle().getMes().intValue() == 1) {
                                int anoAnt = obj.getNominaDetalle().getAno().intValue() - 1;
                                cobParamGeneral = (CobParamGeneral) cacheService.get(CacheService.REGION_COBPARAMGENERAL, "SMMLV" + anoAnt);
                            }
                            //rstSmml = new BigDecimal("0");
                            if (cobParamGeneral != null && cobParamGeneral.getValor() != 0) {
                                rstSmml = new BigDecimal(cobParamGeneral.getValor().toString());
                            }
                            BigDecimal sumatoria = sumValorReglas(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_PAGOS_NOM_SALUD" + anyoMesDetalleKey(obj)),
                                    infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_PERMISOS_REMUNERADOS" + anyoMesDetalleKey(obj)),
                                    infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_SUSP_PERMISOS" + anyoMesDetalleKey(obj)),
                                    infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_VACACIONES" + anyoMesDetalleKey(obj)),
                                    infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_HUELGA" + anyoMesDetalleKey(obj)));

                            CobParamGeneral cobParamGeneral1 = (CobParamGeneral) cacheService.get(CacheService.REGION_COBPARAMGENERAL, "MAXIBCSALUD" + obj.getNominaDetalle().getAno().toString());
                            BigDecimal topeIbcSalud = new BigDecimal("0");
                            if (cobParamGeneral1 != null && cobParamGeneral1.getValor() != 0) {
                                topeIbcSalud = new BigDecimal(cobParamGeneral1.getValor().toString());
                            }
                            BigDecimal mulSmmlIbcTope = topeIbcSalud.multiply(rstSmml);
                            if (obj.getNominaDetalle().getIbcConcurrenciaIngresosOtrosAportantes().doubleValue() >= mulSmmlIbcTope.doubleValue()) {
                                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_SALUD" + anyoMesDetalleKey(obj), new BigDecimal("0"));
                            } else {
                                BigDecimal nuevaSumatoria = sumatoria.add(obj.getNominaDetalle().getIbcConcurrenciaIngresosOtrosAportantes());
                                BigDecimal diaSmml = rstSmml.divide(new BigDecimal("30"), 2, RoundingMode.HALF_UP);
                                BigDecimal diasCotSaluddiaSmml = mulValorReglas(diaSmml, infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#DIAS_COT_SALUD" + anyoMesDetalleKey(obj)));
                                BigDecimal valorResultado = new BigDecimal("0");
                                if (nuevaSumatoria.doubleValue() <= mulSmmlIbcTope.doubleValue()) {
                                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_SALUD" + anyoMesDetalleKey(obj), roundValor(sumatoria));
                                    valorResultado = sumatoria;
                                } else {
                                    resta = mulSmmlIbcTope.subtract(obj.getNominaDetalle().getIbcConcurrenciaIngresosOtrosAportantes());
                                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_SALUD" + anyoMesDetalleKey(obj), roundValor(resta));
                                    valorResultado = resta;
                                }
                                if (valorResultado.doubleValue() < rstSmml.doubleValue()) {
                                    if (valorResultado.doubleValue() < diasCotSaluddiaSmml.doubleValue()) {
                                        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_SALUD" + anyoMesDetalleKey(obj), roundValor(diasCotSaluddiaSmml));
                                    } else {
                                        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_SALUD" + anyoMesDetalleKey(obj), roundValor(valorResultado));
                                    }
                                }
                            }

                        }  // FInal del condicional que ingresa cuando NO se cumplen las primeras dos condiciones de INDEPENDIENTES

                        // Código que finaliza Caso Independientes solo para los topes
                    } else { // SI NO es Trabajador Independiente, se deje como estaba
                        BigDecimal sumatoria = sumValorReglas(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_PAGOS_NOM_SALUD" + anyoMesDetalleKey(obj)),
                                infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_PERMISOS_REMUNERADOS" + anyoMesDetalleKey(obj)),
                                infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_SUSP_PERMISOS" + anyoMesDetalleKey(obj)),
                                infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_VACACIONES" + anyoMesDetalleKey(obj)),
                                infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_HUELGA" + anyoMesDetalleKey(obj)));

                        CobParamGeneral cobParamGeneral1 = (CobParamGeneral) cacheService.get(CacheService.REGION_COBPARAMGENERAL, "MAXIBCSALUD" + obj.getNominaDetalle().getAno().toString());
                        BigDecimal topeIbcSalud = new BigDecimal("0");
                        if (cobParamGeneral1 != null && cobParamGeneral1.getValor() != 0) {
                            topeIbcSalud = new BigDecimal(cobParamGeneral1.getValor().toString());
                        }
                        BigDecimal mulSmmlIbcTope = topeIbcSalud.multiply(rstSmml);
                        if (obj.getNominaDetalle().getIbcConcurrenciaIngresosOtrosAportantes().doubleValue() >= mulSmmlIbcTope.doubleValue()) {
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_SALUD" + anyoMesDetalleKey(obj), new BigDecimal("0"));
                        } else {
                            BigDecimal nuevaSumatoria = sumatoria.add(obj.getNominaDetalle().getIbcConcurrenciaIngresosOtrosAportantes());
                            BigDecimal diaSmml = rstSmml.divide(new BigDecimal("30"), 2, RoundingMode.HALF_UP);
                            BigDecimal diasCotSaluddiaSmml = mulValorReglas(diaSmml, infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#DIAS_COT_SALUD" + anyoMesDetalleKey(obj)));
                            BigDecimal valorResultado = new BigDecimal("0");
                            if (nuevaSumatoria.doubleValue() <= mulSmmlIbcTope.doubleValue()) {
                                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_SALUD" + anyoMesDetalleKey(obj), roundValor(sumatoria));
                                valorResultado = sumatoria;
                            } else {
                                resta = mulSmmlIbcTope.subtract(obj.getNominaDetalle().getIbcConcurrenciaIngresosOtrosAportantes());
                                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_SALUD" + anyoMesDetalleKey(obj), roundValor(resta));
                                valorResultado = resta;
                            }
                            if (valorResultado.doubleValue() < rstSmml.doubleValue()) {
                                if (valorResultado.doubleValue() < diasCotSaluddiaSmml.doubleValue()) {
                                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_SALUD" + anyoMesDetalleKey(obj), roundValor(diasCotSaluddiaSmml));
                                } else {
                                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_SALUD" + anyoMesDetalleKey(obj), roundValor(valorResultado));
                                }
                            }
                            if ("21".equals(obj.getNominaDetalle().getTipoCotizante()) || "20".equals(obj.getNominaDetalle().getTipoCotizante())
                                    || "19".equals(obj.getNominaDetalle().getTipoCotizante()) || "12".equals(obj.getNominaDetalle().getTipoCotizante())) {
                                BigDecimal rstMulDia = mulValorReglas(diaSmml, infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#DIAS_COT_SALUD"
                                        + anyoMesDetalleKey(obj)));
                                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_SALUD" + anyoMesDetalleKey(obj), roundValor(rstMulDia));
                            }
                        }
                    }
                    // Si el <aportante> es "2" se hace este bloque de lo contrario se mantiene el cálculo realizado.
                    break;
            }
        }
       
                
        // Regla #17 <TARIFA SALUD>
        // Por defecto se coloca el valor de la tarifa que viene de la tabla
        //System.out.println("::ANDRES1:: ingrese a CREE TARIFA_SALUD: " + tarifaSalud);
        //infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_SALUD" + anyoMesDetalleKey(obj), tarifaSalud);
        BigDecimal cantidadEmp = gestorProgramaDao.obtenerEmpleadoPeriodo(obj.getNominaDetalle());
        SimpleDateFormat formateador = new SimpleDateFormat("dd/MM/yyyy");
        try {
            Date fechaCree = formateador.parse("01/11/2013");
            Date fechaRegistro = formateador.parse("01/" + obj.getNominaDetalle().getMes() + "/" + obj.getNominaDetalle().getAno());
            BigDecimal salarioDevengado_local = convertValorRegla(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TOTAL_DEVENGADO" + anyoMesDetalleKey(obj)));
            cobParamGeneral = (CobParamGeneral) cacheService.get(CacheService.REGION_COBPARAMGENERAL, "SMMLV" + obj.getNominaDetalle().getAno().toString());
            BigDecimal rstSmml_local = new BigDecimal("0");
            if (cobParamGeneral != null && cobParamGeneral.getValor() != 0) {
                rstSmml_local = new BigDecimal(cobParamGeneral.getValor().toString());
            }
            BigDecimal smml10_local = rstSmml_local.multiply(new BigDecimal("10"));
            BigDecimal tarifaSalud = gestorProgramaDao.tarifaSalud(obj.getNomina(), obj.getNominaDetalle());
            if ("21".equals(obj.getNominaDetalle().getTipoCotizante()) || "20".equals(obj.getNominaDetalle().getTipoCotizante())
                    || "12".equals(obj.getNominaDetalle().getTipoCotizante()) || "19".equals(obj.getNominaDetalle().getTipoCotizante())) {
                //System.out.println("::ANDRES1:: ingrese a CREE TARIFA_SALUD: " + tarifaSalud);
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_SALUD" + anyoMesDetalleKey(obj), tarifaSalud);
            } else {// ultimo cambio hablado con Arley
                //Para los demás "TIPO COTIZANTE" el % debe ser la tarifa % de Salud del trabajador más + tarifa % de Salud del 
                //empleador  registradas, en el módulo del CORE ""Asociar Subsistema""  de la sección ""Administrar parámetros generales 2""
                //aplicable al año y mes que se está fiscalizando teniendo en cuenta las fechas de inicio y fin de cada tarifa.
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_SALUD" + anyoMesDetalleKey(obj), tarifaSalud);
                //como lo del CRRE que son las dos excepciones siguientes tienen prioridad se deben dejar de ultimas es decir que 
                //el CREE tiene prioridad sobre la ley 1429. Excepcion 3
                //Para definir el valor ""devengado"" se tendrá en cuenta el valor registrado en el campo ""TOTAL DEVENGADO""
                //3- Si en la columna ""CONDICION ESPECIAL DE EMPRESA""  aparece una de las siguientes condiciones, aplicar la tarifa de acuerdo a la siguiente lista:
                //_______________________________________________________________________________________ 
                //·  “LEY 1429 Col AÑO 1,2”                            =   11%
                //·  “LEY 1429 Col AÑO 3”                                =   11,38%
                //·  “LEY 1429 Col AÑO 4”                                =   11,75%
                //·  “LEY 1429 Col AÑO 5”                                =   12,13%
                //·  “LEY 1429 AGV AÑO 1 – 8”                       =   11%
                //·  “LEY 1429 AGV AÑO 9”                             =   11,75%
                //·  “LEY 1429 AGV AÑO 10”                           =   12,13% ,
                //·  “Soc.declaradas ZF. Art20 Ley1607”    =   12,5%
                //_______________________________________________________________________________________ 
                if ("LEY 1429 Col AÑO 1,2".equals(obj.getNominaDetalle().getCondEspEmp())) {
                    //System.out.println("::ANDRES7:: ingrese a CREE TARIFA_SALUD: " + new BigDecimal("11"));
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_SALUD" + anyoMesDetalleKey(obj), new BigDecimal("11"));
                }
                if ("LEY 1429 Col AÑO 3".equals(obj.getNominaDetalle().getCondEspEmp())) {
                    //System.out.println("::ANDRES8:: ingrese a CREE TARIFA_SALUD: " + new BigDecimal("11.38"));
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_SALUD" + anyoMesDetalleKey(obj), new BigDecimal("11.38"));
                }
                if ("LEY 1429 Col AÑO 4".equals(obj.getNominaDetalle().getCondEspEmp())) {
                    //System.out.println("::ANDRES9:: ingrese a CREE TARIFA_SALUD: " + new BigDecimal("11.75"));
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_SALUD" + anyoMesDetalleKey(obj), new BigDecimal("11.75"));
                }
                if ("LEY 1429 Col AÑO 5".equals(obj.getNominaDetalle().getCondEspEmp())) {
                    //System.out.println("::ANDRES10:: ingrese a CREE TARIFA_SALUD: " + new BigDecimal("12.13"));
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_SALUD" + anyoMesDetalleKey(obj), new BigDecimal("12.13"));
                }
                if ("LEY 1429 AGV AÑO 1-8".equals(obj.getNominaDetalle().getCondEspEmp())) {
                    //System.out.println("::ANDRES11:: ingrese a CREE TARIFA_SALUD: " + new BigDecimal("11"));
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_SALUD" + anyoMesDetalleKey(obj), new BigDecimal("11"));
                }
                if ("LEY 1429 AGV AÑO 9".equals(obj.getNominaDetalle().getCondEspEmp())) {
                    //System.out.println("::ANDRES12:: ingrese a CREE TARIFA_SALUD: " + new BigDecimal("11.75"));
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_SALUD" + anyoMesDetalleKey(obj), new BigDecimal("11.75"));
                }
                if ("LEY 1429 AGV AÑO 10".equals(obj.getNominaDetalle().getCondEspEmp())) {
                    //System.out.println("::ANDRES13:: ingrese a CREE TARIFA_SALUD: " + new BigDecimal("12.13"));
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_SALUD" + anyoMesDetalleKey(obj), new BigDecimal("12.13"));
                }
                if ("Soc.declaradas ZF. Art20 Ley1607".equals(obj.getNominaDetalle().getCondEspEmp())) {
                    //System.out.println("::ANDRES14:: ingrese a CREE TARIFA_SALUD: " + new BigDecimal("12.5"));
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_SALUD" + anyoMesDetalleKey(obj), new BigDecimal("12.5"));
                }
                if ("SI".equals(obj.getNominaDetalle().getIdaportante().getSujetoPasivoImpuestoCree())
                        && tipoIdentificacionAportante.intValue() == 2) {
                    Date fecha1 = formateador.parse("30/11/2013"); // entre diciembre 2013
                    Date fecha2 = formateador.parse("01/01/2017"); // diciembre 2016
                    Date fecha3 = formateador.parse("31/12/2016"); // a partir de enero 2017
                    //excepcion 1
                    //1- Por efectos de la Ley 1607 de 2012, si el campo “SUJETO PASIVO DEL IMPUESTO SOBRE LA RENTA PARA LA EQUIDAD CREE” 
                    //está marcado con ""SI"", el campo ""TIPO DOCUMENTO APORTANTE"" es igual a ""NI""  y  el periodo fiscalizado es posterior 
                    //a noviembre de 2013, se aplicara la tarifa del 4%  a los trabajadores que ""devenguen"", individualmente considerados, 
                    //hasta diez (10) salarios mínimos mensuales legales vigentes.
                    if (fechaRegistro.after(fecha1) && fechaRegistro.before(fecha2) && salarioDevengado_local.doubleValue() <= smml10_local.doubleValue()) {
                        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_SALUD" + anyoMesDetalleKey(obj), new BigDecimal("4"));
                    }
                    //1- Si el campo “SUJETO PASIVO DEL IMPUESTO SOBRE LA RENTA PARA LA EQUIDAD CREE” 
                    //está marcado con "SI" y  el campo "TIPO DOCUMENTO APORTANTE" es igual a "NI", se aplicará 
                    //la tarifa del 4% así:
                    //a- entre diciembre 2013  y  diciembre 2016, a trabajadores que "devenguen", individualmente considerados, hasta diez (10) SMLMV (Ley 1607/2012)
                    //b- a partir de enero 2017 a trabajadores que "devenguen", individualmente considerados, menos de diez (10) SMLMV (Ley 1819/2016)
                    if (fechaRegistro.after(fecha3) && salarioDevengado_local.doubleValue() < smml10_local.doubleValue()) {
                        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_SALUD" + anyoMesDetalleKey(obj), new BigDecimal("4"));
                    }
                }
                //excepcion 2
                //2- si el campo “SUJETO PASIVO DEL IMPUESTO SOBRE LA RENTA PARA LA EQUIDAD CREE” 
                //está marcado con ""SI"", el campo ""TIPO DOCUMENTO APORTANTE"" es diferente a ""NI"",  
                //el periodo fiscalizado es posterior a noviembre de 2013, y existe mas de un trabajador 
                //en el mes de la nomina que se esta fiscalizando se aplicara la tarifa del 4%  a los trabajadores
                //que ""devenguen"", individualmente considerados, menos de diez (10) salarios mínimos
                //mensuales legales vigentes.
                if ("SI".equals(obj.getNominaDetalle().getIdaportante().getSujetoPasivoImpuestoCree()) && tipoIdentificacionAportante.intValue() != 2
                        && fechaRegistro.after(fechaCree) && cantidadEmp.compareTo(BigDecimal.ONE) > 1
                        && salarioDevengado_local.doubleValue() < smml10_local.doubleValue()) {
                    //System.out.println("::ANDRES16:: ingrese a CREE TARIFA_SALUD: " + new BigDecimal("4"));
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_SALUD" + anyoMesDetalleKey(obj), new BigDecimal("4"));
                }
            }
        } catch (ParseException ex) {
            Logger.getLogger(NominaModoEjecucion.class.getName()).log(Level.SEVERE, null, ex);
        }
        // Regla #18 <TARIFA SALUD PARA SUSPENSIONES>
        String mesNomina;
        Date fechaCree1 = null;
        Date fechaCree2 = null;
        if(obj.getNominaDetalle().getMes().intValue() > 9)
            mesNomina =  obj.getNominaDetalle().getMes().toString();
        else
            mesNomina =  "0" + obj.getNominaDetalle().getMes();
        
        Date fechaNomina = null;
        try {
            fechaNomina = formateador.parse("01/"+ mesNomina  +"/" + obj.getNominaDetalle().getAno());
        } catch (ParseException ex) {
            Logger.getLogger(NominaModoEjecucion.class.getName()).log(Level.SEVERE, null, ex);
        }
        BigDecimal tarifaSaludEmpleador = gestorProgramaDao.tarifaSaludEmpleador(obj.getNomina(), obj.getNominaDetalle());
        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_SALUD_SUSPENSION" + anyoMesDetalleKey(obj), tarifaSaludEmpleador);
        BigDecimal salarioDevengado = convertValorRegla(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TOTAL_REMUNERADO" + anyoMesDetalleKey(obj)));
        // SAlario Minimo buscado en Cache
        cobParamGeneral = (CobParamGeneral) cacheService.get(CacheService.REGION_COBPARAMGENERAL, "SMMLV" + obj.getNominaDetalle().getAno().toString());
        BigDecimal rstSmml = new BigDecimal("0");
        if (cobParamGeneral != null && cobParamGeneral.getValor() != 0) {
            rstSmml = new BigDecimal(cobParamGeneral.getValor().toString());
        }
        BigDecimal smml10 = rstSmml.multiply(new BigDecimal("10"));
        //BigDecimal cantidadEmp = gestorProgramaDao.obtenerEmpleadoPeriodo(obj.getNominaDetalle());
        // tipoIde = gestorProgramaDao.tipoIdentificacionAportante(obj.getNominaDetalle());
        //CASO CREE
        if ("SI".equals(obj.getNominaDetalle().getIdaportante().getSujetoPasivoImpuestoCree())){
            if(tipoIdentificacionAportante.intValue() == 2){     
                try { //periodo fiscalizado es posterior a noviembre de 2013
                    fechaCree1 = formateador.parse("30/11/2013");
                    if(fechaNomina != null && fechaNomina.after(fechaCree1) && salarioDevengado.doubleValue() <= smml10.doubleValue()) {
                        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_SALUD_SUSPENSION" + anyoMesDetalleKey(obj), new BigDecimal("0"));
                        //System.out.println("::ANDRES93:: getSujetoPasivoImpuestoCree : " + 0);
                    }
                } catch (ParseException ex) {
                     Logger.getLogger(NominaModoEjecucion.class.getName()).log(Level.SEVERE, null, ex);
                }
            }else{
                try {//eperiodo fiscalizado es posterior a noviembre de 2013
                    fechaCree1 = formateador.parse("30/11/2013");
                    if(fechaNomina != null && fechaNomina.after(fechaCree1) && cantidadEmp.compareTo(BigDecimal.ONE) == 1  && salarioDevengado.doubleValue() <= smml10.doubleValue()){
                        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_SALUD_SUSPENSION" + anyoMesDetalleKey(obj), new BigDecimal("0"));
                        //System.out.println("::ANDRES93:: getSujetoPasivoImpuestoCree : " + 0);
                    }
                } catch (ParseException ex) {
                    Logger.getLogger(NominaModoEjecucion.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        // Regla #19 <COTIZACION OBLIGATORIA CALCULADA SALUD>
        // Abril 19.2016 Antes esta regla estaba en BD
        BigDecimal tmp1 = minusValorReglas(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_SALUD" + anyoMesDetalleKey(obj)),
        infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_SUSP_PERMISOS" + anyoMesDetalleKey(obj)));
        BigDecimal tmp2 = mulValorReglas(tmp1, infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_SALUD" + anyoMesDetalleKey(obj)));
        BigDecimal resulTmp1 = tmp2.divide(new BigDecimal("100"), 2, RoundingMode.HALF_UP);
        BigDecimal multTmpTarifaSusp = mulValorReglas(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_SUSP_PERMISOS"
                + anyoMesDetalleKey(obj)), infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_SALUD_SUSPENSION" + anyoMesDetalleKey(obj)));
        BigDecimal resulTmp2 = multTmpTarifaSusp.divide(new BigDecimal("100"), 2, RoundingMode.HALF_UP);
        BigDecimal total = resulTmp2.add(resulTmp1);
        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_OBL_CALCULADA_SALUD" + anyoMesDetalleKey(obj), roundValor100(total));
        // Regla #24 <AJUSTE SALUD>
        BigDecimal ajuSalud = convertValorRegla(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_OBL_CALCULADA_SALUD" + anyoMesDetalleKey(obj)));
        BigDecimal cotiPagPilaSalud = convertValorRegla(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_PAGADA_PILA_SALUD" + anyoMesDetalleKey(obj)));
        ajuSalud = ajuSalud.subtract(cotiPagPilaSalud);
        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#AJUSTE_SALUD" + anyoMesDetalleKey(obj), ajuSalud);
        // Regla #25 <CONCEPTO AJUSTE SALUD>
        BigDecimal ajusteSalud = convertValorRegla(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#AJUSTE_SALUD" + anyoMesDetalleKey(obj)));
        if (ajusteSalud == null) {
            ajusteSalud = BigDecimal.ZERO;
        }
        if("X".equals(obj.getNominaDetalle().getOmisionSalud()))
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CONCEPTO_AJUSTE_SALUD" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.OMISO_DESC);
        else{
            if (ajusteSalud.doubleValue() >= 1000){
                BigDecimal cotPagPilaSalud = convertValorRegla(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_PAGADA_PILA_SALUD" + anyoMesDetalleKey(obj)));
                if (cotPagPilaSalud == null) {
                    cotPagPilaSalud = BigDecimal.ZERO;
                }
                if (infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COD_ADM_SALUD" + anyoMesDetalleKey(obj)) == null && cotPagPilaSalud.compareTo(BigDecimal.ZERO) == 0) {
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CONCEPTO_AJUSTE_SALUD" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.OMISO_DESC);
                } else if (infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COD_ADM_SALUD" + anyoMesDetalleKey(obj)) != null && cotPagPilaSalud.compareTo(BigDecimal.ZERO) == 0) {
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CONCEPTO_AJUSTE_SALUD" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.MORA_DESC);
                } else if (cotPagPilaSalud.compareTo(BigDecimal.ZERO) != 0) {
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CONCEPTO_AJUSTE_SALUD" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.INEXACTO_DESC);
                }
            }
        }
        // Regla #26 <TIPO DE INCUMPLIMIENTO SALUD>
        ajusteSalud = convertValorRegla(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#AJUSTE_SALUD" + anyoMesDetalleKey(obj)));
        if (ajusteSalud.intValue() >= 1000) {
            String conceAjuSalud = (String) infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CONCEPTO_AJUSTE_SALUD" + anyoMesDetalleKey(obj));
            if (null != conceAjuSalud) {
                switch (conceAjuSalud) {
                    case ConstantesGestorPrograma.OMISO_DESC:
                        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TIPO_INCUMPLIMIENTO_SALUD" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.OMISO_NOMBRE);
                        break;
                    case ConstantesGestorPrograma.MORA_DESC:
                        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TIPO_INCUMPLIMIENTO_SALUD" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.MORA_NOMBRE);
                        break;
                    case ConstantesGestorPrograma.INEXACTO_DESC:
                        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TIPO_INCUMPLIMIENTO_SALUD" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.INEXACTO_NOMBRE);
                        break;
                    default:
                        break;
                }
            }  
        }
        // Regla #30 <IBC PAGOS EN NOMINA PENSION>
        sumNOTpIncapacidad = gestorProgramaDao.sumaValorLiqConceptoContableIbcPagosNomSaludNoTpIncapacidad(obj.getNomina(), obj.getNominaDetalle(), "2");
        sumNOTpIncapacidad = sumValorReglas(sumNOTpIncapacidad, infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#EXC_LIM_PAGO_NO_SALARIAL" + anyoMesDetalleKey(obj)));
        sumTpIncapacidad = gestorProgramaDao.sumaValorLiqConceptoContableIbcPagosNomPension(obj.getNomina(), obj.getNominaDetalle());

        if (StringUtils.containsIgnoreCase("X", obj.getNominaDetalle().getSalarioIntegral())) {
            BigDecimal por70 = sumNOTpIncapacidad.multiply(new BigDecimal("70"));// FIXME valor quemado
            por70 = por70.divide(new BigDecimal("100"), 2, RoundingMode.HALF_UP);
            sumNOTpIncapacidad = por70;
        }
        sumNOTpIncapacidad = sumNOTpIncapacidad.add(sumTpIncapacidad);
        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_PAGOS_NOM_PENSION" + anyoMesDetalleKey(obj), roundValor(sumNOTpIncapacidad));

        // Regla #31 <TOTAL IBC CALCULADO PENSION>
        String tienePension = gestorProgramaDao.tienePension(obj.getNominaDetalle());
        String tienePensionCotizante = gestorProgramaDao.tienePensionCotizante(obj.getNominaDetalle());

        // SAlario Minimo buscado en Cache
        cobParamGeneral = (CobParamGeneral) cacheService.get(CacheService.REGION_COBPARAMGENERAL, "SMMLV" + obj.getNominaDetalle().getAno().toString());
        rstSmml = new BigDecimal("0");
        if (cobParamGeneral != null && cobParamGeneral.getValor() != 0) {
            rstSmml = new BigDecimal(cobParamGeneral.getValor().toString());
        }

        BigDecimal valorResultante = new BigDecimal("0");
        if (tienePension == null && tienePensionCotizante == null) {
            try {
                ErrorTipo errorObj = new ErrorTipo();
                errorObj.setCodError("IBC_CALCULADO_PENSION");
                errorObj.setValDescError("No encontro codigo SUBTIPO_COTIZANTE. Nominadetalle = " + obj.getNominaDetalle().getId());
                errorTipo.add(errorObj);
                throw new Exception("EXCEPTION ERROR procesarReglasNoFormula: No encontro codigo SUBTIPO_COTIZANTE. Nominadetalle = " + obj.getNominaDetalle().getId());
            } catch (Exception ex) {
                Logger.getLogger(NominaModoEjecucion.class.getName()).log(Level.SEVERE, null, ex);
            }
        } else if ("NO".equals(tienePension) || "NO".equals(tienePensionCotizante) || "X".equals(obj.getNominaDetalle().getExtranjero_no_obligado_a_cotizar_pension())) {
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_PENSION" + anyoMesDetalleKey(obj), new BigDecimal("0"));
        } else {
            if (mapMallaval != null && StringUtils.contains("NO", mapMallaval.get("TIENE_PENSION")) && StringUtils.contains("NO", mapMallaval.get("ALTO_RIESGO"))
                    && StringUtils.contains("NO", mapMallaval.get("TIENE_FSP"))) {
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_PENSION" + anyoMesDetalleKey(obj), new BigDecimal("0"));
            } else {
                // WROJAS - Caso de trabajadores independientes. Oct.08.2019
                if ("2".equals(obj.getNominaDetalle().getIdaportante().getTipoAportante())) {
                    // Se busca el total del ingreso bruto - Tipo de pago
                    boolean seCumpleIBCPension = false;
                    BigDecimal sumIngresoBruto = gestorProgramaDao.sumaValorLiqConceptoContableTotalIBCCalculadoSaludIngresoBruto(obj.getNomina(), obj.getNominaDetalle());
                    formateador = new SimpleDateFormat("dd/MM/yyyy");
                    try {
                        Date fechaControlTrabIndep1 = formateador.parse("30/06/2015");
                        Date fechaControlTrabIndep2 = formateador.parse("30/06/2015");
                        Date fechaRegistroTrabIndep = formateador.parse("01/" + obj.getNominaDetalle().getMes() + "/" + obj.getNominaDetalle().getAno());
                        if (fechaRegistroTrabIndep.before(fechaControlTrabIndep1) && sumIngresoBruto.doubleValue() <= 0) {
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_PENSION" + anyoMesDetalleKey(obj), new BigDecimal("0"));
                            seCumpleIBCPension = true;
                        }
                        if (fechaRegistroTrabIndep.after(fechaControlTrabIndep2) && sumIngresoBruto.doubleValue() < rstSmml.doubleValue()) {
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_PENSION" + anyoMesDetalleKey(obj), new BigDecimal("0"));
                            seCumpleIBCPension = true;
                        }
                    } catch (ParseException ex) {
                        Logger.getLogger(NominaModoEjecucion.class.getName()).log(Level.SEVERE, null, ex);
                    }

                    // Ahora se verifican los topes
                    if (!seCumpleIBCPension) { // Sino se cumplen las primeras dos condiciones se controla con los topes
                        if (obj.getNominaDetalle().getMes().intValue() == 1) {
                            int anoAnt = obj.getNominaDetalle().getAno().intValue() - 1;
                            cobParamGeneral = (CobParamGeneral) cacheService.get(CacheService.REGION_COBPARAMGENERAL, "SMMLV" + anoAnt);
                        }
                        //rstSmml = new BigDecimal("0");
                        if (cobParamGeneral != null && cobParamGeneral.getValor() != 0) {
                            rstSmml = new BigDecimal(cobParamGeneral.getValor().toString());
                        }

                        BigDecimal sumatoria = sumValorReglas(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_PAGOS_NOM_PENSION" + anyoMesDetalleKey(obj)),
                                infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_PERMISOS_REMUNERADOS" + anyoMesDetalleKey(obj)),
                                infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_VACACIONES" + anyoMesDetalleKey(obj)),
                                infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_HUELGA" + anyoMesDetalleKey(obj)));
                        BigDecimal ibcPensionConcurrencia = convertValorRegla(obj.getNominaDetalle().getIbcPenConIngrOtrApo());
                        BigDecimal sumatoriaConIbcPension = sumatoria.add(ibcPensionConcurrencia);

                        CobParamGeneral cobParamGeneral1 = (CobParamGeneral) cacheService.get(CacheService.REGION_COBPARAMGENERAL, "MAXIBCPENSION" + obj.getNominaDetalle().getAno().toString());
                        BigDecimal topeIbcPension = new BigDecimal("0");
                        if (cobParamGeneral1 != null && cobParamGeneral1.getValor() != 0) {
                            topeIbcPension = new BigDecimal(cobParamGeneral1.getValor().toString());
                        }
                        BigDecimal rstSmmlPorTopeIbcPension = topeIbcPension.multiply(rstSmml);
                        tmp2 = rstSmml.divide(new BigDecimal("30"), 2, RoundingMode.HALF_UP);
                        if (ibcPensionConcurrencia.doubleValue() >= rstSmmlPorTopeIbcPension.doubleValue()) {
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_PENSION" + anyoMesDetalleKey(obj), new BigDecimal("0"));
                        } else {
                            if (sumatoriaConIbcPension.doubleValue() <= rstSmmlPorTopeIbcPension.doubleValue()) {
                                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_PENSION" + anyoMesDetalleKey(obj), roundValor(sumatoria));
                                valorResultante = sumatoria;
                            } else {
                                BigDecimal resto = rstSmmlPorTopeIbcPension.subtract(ibcPensionConcurrencia);
                                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_PENSION" + anyoMesDetalleKey(obj), roundValor(resto));
                                valorResultante = resto;
                            }
                            BigDecimal diasCotPensiondiaSmml = mulValorReglas(tmp2, infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#DIAS_COT_PENSION" + anyoMesDetalleKey(obj)));
                            if (valorResultante.doubleValue() < rstSmml.doubleValue()) {
                                if (valorResultante.doubleValue() < diasCotPensiondiaSmml.doubleValue()) {
                                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_PENSION" + anyoMesDetalleKey(obj), roundValor(diasCotPensiondiaSmml));
                                } else {
                                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_PENSION" + anyoMesDetalleKey(obj), roundValor(valorResultante));
                                }
                            }
                        }

                    } // Final del condicional al cual se ingresa cuando NO se cumplen las primeras dos condiciones

                    // Aqui termina la verificación de los topes
                } else { // Por este lado del <else> ingresa cuando NO es independientes
                    BigDecimal sumatoria = sumValorReglas(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_PAGOS_NOM_PENSION" + anyoMesDetalleKey(obj)),
                            infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_PERMISOS_REMUNERADOS" + anyoMesDetalleKey(obj)),
                            infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_VACACIONES" + anyoMesDetalleKey(obj)),
                            infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_HUELGA" + anyoMesDetalleKey(obj)));
                    BigDecimal ibcPensionConcurrencia = convertValorRegla(obj.getNominaDetalle().getIbcPenConIngrOtrApo());
                    BigDecimal sumatoriaConIbcPension = sumatoria.add(ibcPensionConcurrencia);

                    CobParamGeneral cobParamGeneral1 = (CobParamGeneral) cacheService.get(CacheService.REGION_COBPARAMGENERAL, "MAXIBCPENSION" + obj.getNominaDetalle().getAno().toString());
                    BigDecimal topeIbcPension = new BigDecimal("0");
                    if (cobParamGeneral1 != null && cobParamGeneral1.getValor() != 0) {
                        topeIbcPension = new BigDecimal(cobParamGeneral1.getValor().toString());
                    }
                    BigDecimal rstSmmlPorTopeIbcPension = topeIbcPension.multiply(rstSmml);
                    tmp2 = rstSmml.divide(new BigDecimal("30"), 2, RoundingMode.HALF_UP);

                    if (ibcPensionConcurrencia.doubleValue() >= rstSmmlPorTopeIbcPension.doubleValue()) {
                        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_PENSION" + anyoMesDetalleKey(obj), new BigDecimal("0"));
                    } else {
                        if (sumatoriaConIbcPension.doubleValue() <= rstSmmlPorTopeIbcPension.doubleValue()) {
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_PENSION" + anyoMesDetalleKey(obj), roundValor(sumatoria));
                            valorResultante = sumatoria;
                        } else {
                            BigDecimal resto = rstSmmlPorTopeIbcPension.subtract(ibcPensionConcurrencia);
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_PENSION" + anyoMesDetalleKey(obj), roundValor(resto));
                            valorResultante = resto;
                        }
                        BigDecimal diasCotPensiondiaSmml = mulValorReglas(tmp2, infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#DIAS_COT_PENSION" + anyoMesDetalleKey(obj)));
                        if (valorResultante.doubleValue() < rstSmml.doubleValue()) {
                            if (valorResultante.doubleValue() < diasCotPensiondiaSmml.doubleValue()) {
                                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_PENSION" + anyoMesDetalleKey(obj), roundValor(diasCotPensiondiaSmml));
                            } else {
                                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_PENSION" + anyoMesDetalleKey(obj), roundValor(valorResultante));
                            }
                        }
                        if ("20".equals(obj.getNominaDetalle().getTipoCotizante())) {
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_PENSION" + anyoMesDetalleKey(obj), roundValor(diasCotPensiondiaSmml));
                        }
                    }

                } // Aqui termina el caso del IBC para empresas -> cuando ingresa al <else>

            }
        }
        
        // Regla #32 <TARIFA PENSION>
        rst = gestorProgramaDao.tarifaPensionPorcentual(obj.getNomina(), obj.getNominaDetalle());   
        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_PENSION" + anyoMesDetalleKey(obj), rst);
        // Regla #33 <COTIZACION OBLIGATORIA PENSION>
        rst = mulValorReglas(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_PENSION" + anyoMesDetalleKey(obj)),
                infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_PENSION" + anyoMesDetalleKey(obj)));
        rst = rst.divide(new BigDecimal("100"));
        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_OBL_PENSION" + anyoMesDetalleKey(obj), roundValor100(rst));
        // Regla #38 <AJUSTE PENSION>
        if ("X".equals(obj.getNominaDetalle().getCalculoActuarial())) {
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#AJUSTE_PENSION" + anyoMesDetalleKey(obj), new Integer("0"));
        } else {
            BigDecimal ajustePension = minusValorReglas(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_OBL_PENSION" + anyoMesDetalleKey(obj)),
                    infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COT_PAGADA_PILA_PENSION" + anyoMesDetalleKey(obj)));
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#AJUSTE_PENSION" + anyoMesDetalleKey(obj), ajustePension);
        }
        // Regla #39 <CONCEPTO AJUSTE PENSION>
        BigDecimal ajustePension = convertValorRegla(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#AJUSTE_PENSION" + anyoMesDetalleKey(obj)));
        if (ajustePension == null) {
            ajustePension = BigDecimal.ZERO;
        }
        //OJO - Validar que primeramente no sean nulos y colocar ceros
        if("X".equals(obj.getNominaDetalle().getOmisionPension()))
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CONCEPTO_AJUSTE_PENSION" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.OMISO_DESC);
        else{
            if (ajustePension.intValue() >= 1000 || "X".equals(obj.getNominaDetalle().getCalculoActuarial())) {
                BigDecimal cotPagadaPilaPension = convertValorRegla(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COT_PAGADA_PILA_PENSION" + anyoMesDetalleKey(obj)));
                if (cotPagadaPilaPension == null) {
                    cotPagadaPilaPension = BigDecimal.ZERO;
                }
                if ("X".equals(obj.getNominaDetalle().getCalculoActuarial())) {
                    // FIXME verificar el nombre que se le va a dar a esta variable
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CONCEPTO_AJUSTE_PENSION" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.VDCA_DESC);
                } else if (infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COD_ADM_PENSION" + anyoMesDetalleKey(obj)) == null && cotPagadaPilaPension.compareTo(BigDecimal.ZERO) == 0) {
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CONCEPTO_AJUSTE_PENSION" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.OMISO_DESC);
                } else if (infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COD_ADM_PENSION" + anyoMesDetalleKey(obj)) != null && cotPagadaPilaPension.compareTo(BigDecimal.ZERO) == 0) {
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CONCEPTO_AJUSTE_PENSION" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.MORA_DESC);
                } else if (cotPagadaPilaPension.compareTo(BigDecimal.ZERO) != 0) {
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CONCEPTO_AJUSTE_PENSION" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.INEXACTO_DESC);
                }
            }
        }
        
        
       // REVISIÓN WILSON ROJAS POR AQUI
        // Regla #40 <TIPO DE INCUMPLIMIENTO PENSION>
        ajustePension = convertValorRegla(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#AJUSTE_PENSION" + anyoMesDetalleKey(obj)));

        if (ajustePension.intValue() >= 1000 || "X".equals(obj.getNominaDetalle().getCalculoActuarial())) {

            String conceAjuPension = (String) infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CONCEPTO_AJUSTE_PENSION" + anyoMesDetalleKey(obj));

            if (null != conceAjuPension) {
                switch (conceAjuPension) {
                    case ConstantesGestorPrograma.OMISO_DESC:
                    case ConstantesGestorPrograma.VDCA_DESC:
                        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TIPO_INCUMPLIMIENTO_PENSION" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.OMISO_NOMBRE);
                        break;
                    case ConstantesGestorPrograma.MORA_DESC:
                        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TIPO_INCUMPLIMIENTO_PENSION" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.MORA_NOMBRE);
                        break;
                    case ConstantesGestorPrograma.INEXACTO_DESC:
                        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TIPO_INCUMPLIMIENTO_PENSION" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.INEXACTO_NOMBRE);
                        break;
                    default:
                        break;
                }
            }
        }

        // Regla #42 <TARIFA FSP SUBCUENTA DE SOLIDARIDAD>
        
        
        rst = gestorProgramaDao.tarifaFspSolidaridad(obj.getNominaDetalle(),
                convertValorRegla(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_PENSION" + anyoMesDetalleKey(obj))));
        
        
        /*
        if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("12345") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 1)
        {
            System.out.println("::ANDRES43:: TARIFA_FSP_SUBCUEN_SOLIDARIDAD : " + rst);
            System.out.println("::ANDRES44:: IBC_CALCULADO_PENSION : " + infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_PENSION" + anyoMesDetalleKey(obj))); 
        }
        */
        
        
        
        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_FSP_SUBCUEN_SOLIDARIDAD" + anyoMesDetalleKey(obj), rst);

        
        
        // Regla #43 <TARIFA FSP SUBCUENTA DE SUBSISTENCIA>
        rst = gestorProgramaDao.tarifaFspSubsistencia(obj.getNomina(), obj.getNominaDetalle(), convertValorRegla(infoNegocio
                .get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_PENSION" + anyoMesDetalleKey(obj))));
        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_FSP_SUBCUEN_SUBSISTEN" + anyoMesDetalleKey(obj), rst);

        // Regla #44 <COTIZACION OBLIGATORIA FSP SUBCUENTA DE SOLIDARIDAD>
        rst = mulValorReglas(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_PENSION" + anyoMesDetalleKey(obj)),
                infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_FSP_SUBCUEN_SOLIDARIDAD" + anyoMesDetalleKey(obj)));
        rst = rst.divide(new BigDecimal("100"), 2, RoundingMode.HALF_UP);
        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_OBL_FSP_SUB_SOLIDARIDAD" + anyoMesDetalleKey(obj), roundValor100(rst));

        // Regla #45 <COTIZACION OBLIGATORIA FSP SUBCUENTA DE SUBSISTENCIA>
        rst = mulValorReglas(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_PENSION" + anyoMesDetalleKey(obj)),
                infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_FSP_SUBCUEN_SUBSISTEN" + anyoMesDetalleKey(obj)));

        rst = rst.divide(new BigDecimal("100"), 2, RoundingMode.HALF_UP);
        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_OBL_FSP_SUB_SUBSISTENCIA" + anyoMesDetalleKey(obj), roundValor100(rst));

        // Regla #48 <AJUSTE FSP SUBCUENTA DE SOLIDARIDAD>
        if ("X".equals(obj.getNominaDetalle().getCalculoActuarial())) {
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#AJUSTE_FSP_SUBCUEN_SOLIDARIDAD" + anyoMesDetalleKey(obj), new Integer("0"));
        } else {
            BigDecimal ajusteSolid = minusValorReglas(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_OBL_FSP_SUB_SOLIDARIDAD"
                    + anyoMesDetalleKey(obj)), infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_PAG_PILA_FSP_SUB_SOLIDAR"
                    + anyoMesDetalleKey(obj)));
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#AJUSTE_FSP_SUBCUEN_SOLIDARIDAD" + anyoMesDetalleKey(obj), roundValor100(ajusteSolid));
        }

        // Regla #49 <AJUSTE FSP SUBCUENTA DE SUBSISTENCIA>
        if ("X".equals(obj.getNominaDetalle().getCalculoActuarial())) {
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#AJUSTE_FSP_SUBCUEN_SUBSISTEN" + anyoMesDetalleKey(obj), new Integer("0"));
        } else {
            BigDecimal ajusteFsp = minusValorReglas(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_OBL_FSP_SUB_SUBSISTENCIA"
                    + anyoMesDetalleKey(obj)), infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_PAG_PILA_FSP_SUB_SUBSIS"
                    + anyoMesDetalleKey(obj)));
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#AJUSTE_FSP_SUBCUEN_SUBSISTEN" + anyoMesDetalleKey(obj), roundValor100(ajusteFsp));
        }

        
        
        //System.out.println("::ANDRES96:: procesarReglasNoFormula pilaDepurada: ");
        
        
           
        // Regla #50 <CONCEPTO AJUSTE FSP>
        rst = sumValorReglas(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#AJUSTE_FSP_SUBCUEN_SOLIDARIDAD" + anyoMesDetalleKey(obj)),
                infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#AJUSTE_FSP_SUBCUEN_SUBSISTEN" + anyoMesDetalleKey(obj)));



        String conceptoAjtPension = (String) infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CONCEPTO_AJUSTE_PENSION" + anyoMesDetalleKey(obj));

        BigDecimal cotizPagadaPilaFsp = convertValorRegla(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_PAG_PILA_FSP_SUB_SUBSIS" + anyoMesDetalleKey(obj)));

        //System.out.println("::ANDRES11:: rst: " + rst);
        //System.out.println("::ANDRES22:: conceptoAjtPension: " + conceptoAjtPension);
        //System.out.println("::ANDRES33:: cotizPagadaPilaFsp: " + cotizPagadaPilaFsp);
        if (conceptoAjtPension == null) {
            conceptoAjtPension = "NULL";
        }

        if (cotizPagadaPilaFsp == null) {
            cotizPagadaPilaFsp = BigDecimal.ZERO;
        }

        
        //En caso de que la suma de "AJUSTE FSP Subcuenta de Solidaridad" (+)  más el "AJUSTE FSP Subcuenta de subsistencia" 
        //sea mayor o igual a mil pesos $1.000  
        
        if("X".equals(obj.getNominaDetalle().getOmisionPension()))
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CONCEPTO_AJUSTE_FSP" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.OMISO_DESC);
        else
        {
            if (rst.intValue() >= 1000) 
            {

                if (infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COD_ADM_PENSION" + anyoMesDetalleKey(obj)) == null
                        && cotizPagadaPilaFsp.compareTo(BigDecimal.ZERO) == 0) {
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CONCEPTO_AJUSTE_FSP" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.OMISO_DESC);
                } else if (cotizPagadaPilaFsp.compareTo(BigDecimal.ZERO) == 0
                        && infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COD_ADM_PENSION" + anyoMesDetalleKey(obj)) != null
                        && (conceptoAjtPension.equals(ConstantesGestorPrograma.INEXACTO_DESC) || conceptoAjtPension.equals("NULL"))) {
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CONCEPTO_AJUSTE_FSP" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.INEXACTO_DESC);
                } else if (cotizPagadaPilaFsp.compareTo(BigDecimal.ZERO) == 0
                        && infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COD_ADM_PENSION" + anyoMesDetalleKey(obj)) != null
                        && conceptoAjtPension.equals(ConstantesGestorPrograma.MORA_DESC)) {
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CONCEPTO_AJUSTE_FSP" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.MORA_DESC);
                } else if (cotizPagadaPilaFsp.compareTo(BigDecimal.ZERO) != 0) {
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CONCEPTO_AJUSTE_FSP" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.INEXACTO_DESC);
                }



                //MODIFICACION regla #28 <CODIGO ADMINISTRADORA PENSION>  14/08/2017
                //Y  que el “AJUSTE PENSIÓN” sea menor que mil pesos $1000, se debe colocar el código  FSP001
                if(ajustePension.intValue() < 1000)
                {
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COD_ADM_PENSION" + anyoMesDetalleKey(obj), "FSP001");

                    // si piladepurada=null Regla #29 <NOMBRE CORTO ADMINISTRADORA PENSION>
                    String nomAdmCcf = LST_ADMINISTRADORA_PILA.get("FSP001");
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#NOM_ADM_PENSION" + anyoMesDetalleKey(obj), nomAdmCcf);
                }   
            }
        }
        
        // Regla #51 <TIPO DE INCUMPLIMIENTO FSP>
        String conceptoAjuFsp = (String) infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CONCEPTO_AJUSTE_FSP" + anyoMesDetalleKey(obj));

        if (conceptoAjuFsp != null) {
            switch (conceptoAjuFsp) {
                case ConstantesGestorPrograma.OMISO_DESC:
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TIPO_INCUMPLIMIENTO_FSP" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.OMISO_NOMBRE);
                    break;
                case ConstantesGestorPrograma.MORA_DESC:
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TIPO_INCUMPLIMIENTO_FSP" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.MORA_NOMBRE);
                    break;
                case ConstantesGestorPrograma.INEXACTO_DESC:
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TIPO_INCUMPLIMIENTO_FSP" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.INEXACTO_NOMBRE);
                    break;
                default:
                    break;
            }
        }

        
        
        // Regla #53 <TARIFA PENSION ADICIONAL ACT. ALTO RIESGO>
       
        if ("X".equals(obj.getNominaDetalle().getActividad_alto_riesgo_pension())) {
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TAR_PENSION_ACT_ALTORIESGO" + anyoMesDetalleKey(obj), new Integer("10"));
        }else{
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TAR_PENSION_ACT_ALTORIESGO" + anyoMesDetalleKey(obj), new Integer("0"));
        }
        // Regla #54 <COTIZACION OBLIGATORIA ADICIONAL ACT. ALTO RIESGO>
        if ("X".equals(obj.getNominaDetalle().getActividad_alto_riesgo_pension())){
            rst = mulValorReglas(
                    infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CALCULADO_PENSION" + anyoMesDetalleKey(obj)),
                    infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TAR_PENSION_ACT_ALTORIESGO" + anyoMesDetalleKey(obj)));
            rst = rst.divide(new BigDecimal("100"));
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COT_OBL_ADIC_ACT_ALTORIESGO" + anyoMesDetalleKey(obj), roundValor100(rst));
        }else {
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COT_OBL_ADIC_ACT_ALTORIESGO" + anyoMesDetalleKey(obj), new Integer("0"));
        }
        
        
 

        // Regla #55 <TARIFA PILA PENSION ADICIONAL ACT. ALTO RIESGO>
        if (pilaDepurada != null)                                                                                                                      
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TAR_PILA_PENSION_ACT_ALTORIES" + anyoMesDetalleKey(obj), pilaDepurada.getTariPilaPensAdAltoRiesgo());
        else
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TAR_PILA_PENSION_ACT_ALTORIES" + anyoMesDetalleKey(obj), new Integer("0"));
            
        
        
        // Regla #56 <COTIZACION PAGADA PILA PENSION ADICIONAL ACT. ALTO RIESGO>
        
        //System.out.println("::ANDRES80:: COT_PAG_PILA_PENSION_ACT_ARIES getNominaDetalle ID: " + obj.getNominaDetalle().getId());
        //System.out.println("::ANDRES81:: COT_PAG_PILA_PENSION_ACT_ARIES getCargueManualPilaAltoRiPe: " + obj.getNominaDetalle().getCargueManualPilaAltoRiPe().toString());
        if(StringUtils.isNotBlank(obj.getNominaDetalle().getCargueManualPilaAltoRiPe().toString()) && convertValorRegla(obj.getNominaDetalle().getCargueManualPilaAltoRiPe().toString()).intValue() > 0)
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COT_PAG_PILA_PENSION_ACT_ARIES" + anyoMesDetalleKey(obj), obj.getNominaDetalle().getCargueManualPilaAltoRiPe().toString());
        else
        {
            if (pilaDepurada != null)                                                                                                                  //APOR_COR_OBLI_PEN_ALT_RIESGO      
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COT_PAG_PILA_PENSION_ACT_ARIES" + anyoMesDetalleKey(obj), pilaDepurada.getAporCorObliPenAltRiesgo());
            else                                                                                                                                    
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COT_PAG_PILA_PENSION_ACT_ARIES" + anyoMesDetalleKey(obj), new Integer("0"));   
        }

        
        //System.out.println("::ANDRES83:: COT_PAG_PILA_PENSION_ACT_ARIES : " + infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COT_PAG_PILA_PENSION_ACT_ARIES" + anyoMesDetalleKey(obj)));
        //System.out.println("::ANDRES84:: getAporCorObliPenAltRiesgo : " + pilaDepurada.getAporCorObliPenAltRiesgo());
       
        
        
        // Regla #57 <AJUSTE PENSION ADICIONAL ACT. ALTO RIESGO>
        if ("X".equals(obj.getNominaDetalle().getCalculoActuarial())) 
        {
            //System.out.println("::ANDRES87:: COT_OBL_ADIC_ACT_ALTORIESGO getCalculoActuarial: 0");
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#AJUSTE_PENSION_ACT_ALTO_RIES" + anyoMesDetalleKey(obj), new Integer("0"));   
            
        } 
        else 
        {
           
            rst = minusValorReglas(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COT_OBL_ADIC_ACT_ALTORIESGO"
                    + anyoMesDetalleKey(obj)), infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COT_PAG_PILA_PENSION_ACT_ARIES"
                    + anyoMesDetalleKey(obj)));
            
            //System.out.println("::ANDRES84:: rst: " + rst);
            //System.out.println("::ANDRES85:: COT_PAG_PILA_PENSION_ACT_ARIES rst: " + infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COT_PAG_PILA_PENSION_ACT_ARIES" + anyoMesDetalleKey(obj)));
            //System.out.println("::ANDRES86:: COT_OBL_ADIC_ACT_ALTORIESGO rst: " + infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COT_OBL_ADIC_ACT_ALTORIESGO"  + anyoMesDetalleKey(obj)));
            
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#AJUSTE_PENSION_ACT_ALTO_RIES" + anyoMesDetalleKey(obj), roundValor(rst));
        }

        
        
        // Regla #58 <CONCEPTO AJUSTE PENSION ADICIONAL ACT. ALTO RIESGO>
        rst = convertValorRegla(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#AJUSTE_PENSION_ACT_ALTO_RIES" + anyoMesDetalleKey(obj)));

        BigDecimal cotizPagadaPilaPensionAct = convertValorRegla(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COT_PAG_PILA_PENSION_ACT_ARIES" + anyoMesDetalleKey(obj)));

        if (cotizPagadaPilaPensionAct == null) {
            cotizPagadaPilaPensionAct = BigDecimal.ZERO;
        }
  
        
        
        if("X".equals(obj.getNominaDetalle().getOmisionPension()))
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CON_AJUS_PENSION_ACT_ARIESGO" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.OMISO_DESC);
        else
        {
            if (rst.intValue() >= 1000) {
                if (infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COD_ADM_PENSION" + anyoMesDetalleKey(obj)) == null
                        && cotizPagadaPilaPensionAct.compareTo(BigDecimal.ZERO) == 0) {
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CON_AJUS_PENSION_ACT_ARIESGO" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.OMISO_DESC);
                } else if (cotizPagadaPilaPensionAct.compareTo(BigDecimal.ZERO) == 0
                        && infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COD_ADM_PENSION" + anyoMesDetalleKey(obj)) != null) {
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CON_AJUS_PENSION_ACT_ARIESGO" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.MORA_DESC);
                } else if (cotizPagadaPilaPensionAct.compareTo(BigDecimal.ZERO) != 0) {
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CON_AJUS_PENSION_ACT_ARIESGO" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.INEXACTO_DESC);
                }
            }
        }
        
        
        
        //System.out.println("::ANDRES97:: procesarReglasNoFormula pilaDepurada: ");

        // Regla #59 <TIPO DE INCUMPLIMIENTO PENSION ADICIONAL ACT. ALTO RIESGO>
        rst = convertValorRegla(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#AJUSTE_PENSION_ACT_ALTO_RIES" + anyoMesDetalleKey(obj)));
        
        if (rst.intValue() >= 1000) 
        {
        
            if (ajusteSalud.intValue() >= 1000) 
            {
                String conceAjuPenActAltoRiesgo = (String) infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CON_AJUS_PENSION_ACT_ARIESGO" + anyoMesDetalleKey(obj));

                if (null != conceAjuPenActAltoRiesgo) 
                {
                    switch (conceAjuPenActAltoRiesgo) {
                        case ConstantesGestorPrograma.OMISO_DESC:
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TIPO_INC_PENSION_ACT_ARIESGO" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.OMISO_NOMBRE);
                            break;
                        case ConstantesGestorPrograma.MORA_DESC:
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TIPO_INC_PENSION_ACT_ARIESGO" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.MORA_NOMBRE);
                            break;
                        case ConstantesGestorPrograma.INEXACTO_DESC:
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TIPO_INC_PENSION_ACT_ARIESGO" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.INEXACTO_NOMBRE);
                            break;
                        default:
                            break;
                    }
                }
            } 

        }


        
        // Regla #61 <CALCULO ACTUARIAL>
        if ("X".equals(obj.getNominaDetalle().getCalculoActuarial())) {
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CALCULO_ACTUARIAL" + anyoMesDetalleKey(obj),
                    obj.getNominaDetalle().getValorCalculoActuarial());
        }

        // Regla #64 <IBC ARL>
        if (obj.getNominaDetalle().getDiasTrabajadosMes().equals(0) || "X".equals(obj.getNominaDetalle().getColombiano_en_el_exterior())) {
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_ARL" + anyoMesDetalleKey(obj), new BigDecimal("0"));
        } else {
            if (mapMallaval != null && StringUtils.contains("NO", mapMallaval.get("TIENE_ARP"))) {
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_ARL" + anyoMesDetalleKey(obj), new BigDecimal("0"));
            } else {
                // WROJAS - Oct.04.2011 RQ: TRabajadores Independientes
                if ("2".equals(obj.getNominaDetalle().getIdaportante().getTipoAportante())) {
                    // Se busca el total del ingreso bruto - Tipo de pago
                    BigDecimal sumIngresoBruto = gestorProgramaDao.sumaValorLiqConceptoContableTotalIBCCalculadoSaludIngresoBruto(obj.getNomina(), obj.getNominaDetalle());
                    formateador = new SimpleDateFormat("dd/MM/yyyy");
                    boolean seCumpleIBCArl = false;
                    try {
                        Date fechaControlTrabIndep1 = formateador.parse("30/06/2015");
                        Date fechaControlTrabIndep2 = formateador.parse("30/06/2015");
                        Date fechaRegistroTrabIndep = formateador.parse("01/" + obj.getNominaDetalle().getMes() + "/" + obj.getNominaDetalle().getAno());
                        if (fechaRegistroTrabIndep.before(fechaControlTrabIndep1) && sumIngresoBruto.doubleValue() <= 0) {
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_ARL" + anyoMesDetalleKey(obj), new BigDecimal("0"));
                            seCumpleIBCArl = true;
                        }
                        if (fechaRegistroTrabIndep.after(fechaControlTrabIndep2) && sumIngresoBruto.doubleValue() < rstSmml.doubleValue()) {
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_ARL" + anyoMesDetalleKey(obj), new BigDecimal("0"));
                            seCumpleIBCArl = true;
                        }
                    } catch (ParseException ex) {
                        Logger.getLogger(NominaModoEjecucion.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    // Aqui inicia la validación de topes para INDEPENDIENTES
                    if (!seCumpleIBCArl) { // SI no se cumplen las condiciones anteriores se verifican los topes
                        tmp1 = gestorProgramaDao.sumaValorLiqConceptoContableNominaDetalle(obj.getNomina(), obj.getNominaDetalle(), "3");
                        tmp2 = sumValorReglas(tmp1, infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#EXC_LIM_PAGO_NO_SALARIAL" + anyoMesDetalleKey(obj)));
                        // SAlario Minimo buscado en Cache
                        cobParamGeneral = (CobParamGeneral) cacheService.get(CacheService.REGION_COBPARAMGENERAL, "SMMLV" + obj.getNominaDetalle().getAno().toString());
                        // Trabajador independiente y es enero
                        if ("2".equals(obj.getNominaDetalle().getIdaportante().getTipoAportante())) {
                            // Dic.04.2019 TIndependientes. Si el mes es enero se busca el SMMLV del año anterior.
                            if (obj.getNominaDetalle().getMes().intValue() == 1) {
                                int anoAnt = obj.getNominaDetalle().getAno().intValue() - 1;
                                cobParamGeneral = (CobParamGeneral) cacheService.get(CacheService.REGION_COBPARAMGENERAL, "SMMLV" + anoAnt);
                            }
                        }
                        rstSmml = new BigDecimal("0");
                        if (cobParamGeneral != null && cobParamGeneral.getValor() != 0) {
                            rstSmml = new BigDecimal(cobParamGeneral.getValor().toString());
                        }
                        BigDecimal topIbcARL = gestorProgramaDao.topeIbcArl(obj.getNomina(), obj.getNominaDetalle());
                        BigDecimal topeMaxIbc = rstSmml.multiply(topIbcARL);
                        BigDecimal resulParcial = tmp2;
                        valorResultante = new BigDecimal("0");
                        if ("X".equals(obj.getNominaDetalle().getSalarioIntegral())) {
                            BigDecimal por70 = tmp2.multiply(new BigDecimal("70"));
                            por70 = por70.divide(new BigDecimal("100"), 2, RoundingMode.HALF_UP);
                            resulParcial = por70;
                        }
                        BigDecimal ibcArlConcurrencia = convertValorRegla(obj.getNominaDetalle().getIbcArlConIngrOtrApo());
                        tmp2 = resulParcial.add(ibcArlConcurrencia);
                        if (ibcArlConcurrencia.doubleValue() >= topeMaxIbc.doubleValue()) {
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_ARL" + anyoMesDetalleKey(obj), new BigDecimal("0"));
                        } else {
                            if (tmp2.doubleValue() <= topeMaxIbc.doubleValue()) {
                                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_ARL" + anyoMesDetalleKey(obj), roundValor(tmp2));
                                valorResultante = tmp2;
                            } else {
                                topeMaxIbc.subtract(ibcArlConcurrencia);
                                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_ARL" + anyoMesDetalleKey(obj), roundValor(topeMaxIbc));
                                valorResultante = topeMaxIbc;
                            }
                            if (valorResultante.doubleValue() < rstSmml.doubleValue()) {
                                BigDecimal diaSmml = rstSmml.divide(new BigDecimal("30"), 2, RoundingMode.HALF_UP);
                                BigDecimal proporcionSalarioMes = diaSmml.multiply(new BigDecimal(obj.getNominaDetalle().getDiasTrabajadosMes()));
                                if (valorResultante.doubleValue() < proporcionSalarioMes.doubleValue()) {
                                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_ARL" + anyoMesDetalleKey(obj), roundValor(proporcionSalarioMes));
                                } else {
                                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_ARL" + anyoMesDetalleKey(obj), roundValor(valorResultante));
                                }
                            }
                        }

                    } // Aqui finaliza el condicional al cual se ingresa cuando NO se cumplen las primeras dos condiciones de INDEPENDIENTES

                } else { // FInaliza ajuste Trabajadores Independientes el <else> es para el caso de empresas
                    tmp1 = gestorProgramaDao.sumaValorLiqConceptoContableNominaDetalle(obj.getNomina(), obj.getNominaDetalle(), "3");
                    tmp2 = sumValorReglas(tmp1, infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#EXC_LIM_PAGO_NO_SALARIAL" + anyoMesDetalleKey(obj)));
                    // SAlario Minimo buscado en Cache
                    cobParamGeneral = (CobParamGeneral) cacheService.get(CacheService.REGION_COBPARAMGENERAL, "SMMLV" + obj.getNominaDetalle().getAno().toString());
                    rstSmml = new BigDecimal("0");
                    if (cobParamGeneral != null && cobParamGeneral.getValor() != 0) {
                        rstSmml = new BigDecimal(cobParamGeneral.getValor().toString());
                    }
                    BigDecimal topIbcARL = gestorProgramaDao.topeIbcArl(obj.getNomina(), obj.getNominaDetalle());
                    BigDecimal topeMaxIbc = rstSmml.multiply(topIbcARL);
                    BigDecimal resulParcial = tmp2;
                    valorResultante = new BigDecimal("0");
                    if ("X".equals(obj.getNominaDetalle().getSalarioIntegral())) {
                        BigDecimal por70 = tmp2.multiply(new BigDecimal("70"));
                        por70 = por70.divide(new BigDecimal("100"), 2, RoundingMode.HALF_UP);
                        resulParcial = por70;
                    }
                    BigDecimal ibcArlConcurrencia = convertValorRegla(obj.getNominaDetalle().getIbcArlConIngrOtrApo());
                    tmp2 = resulParcial.add(ibcArlConcurrencia);
                    if (ibcArlConcurrencia.doubleValue() >= topeMaxIbc.doubleValue()) {
                        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_ARL" + anyoMesDetalleKey(obj), new BigDecimal("0"));
                    } else {
                        if (tmp2.doubleValue() <= topeMaxIbc.doubleValue()) {
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_ARL" + anyoMesDetalleKey(obj), roundValor(tmp2));
                            valorResultante = tmp2;
                        } else {
                            topeMaxIbc.subtract(ibcArlConcurrencia);
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_ARL" + anyoMesDetalleKey(obj), roundValor(topeMaxIbc));
                            valorResultante = topeMaxIbc;
                        }
                        if (valorResultante.doubleValue() < rstSmml.doubleValue()) {
                            BigDecimal diaSmml = rstSmml.divide(new BigDecimal("30"), 2, RoundingMode.HALF_UP);
                            BigDecimal proporcionSalarioMes = diaSmml.multiply(new BigDecimal(obj.getNominaDetalle().getDiasTrabajadosMes()));
                            if (valorResultante.doubleValue() < proporcionSalarioMes.doubleValue()) {
                                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_ARL" + anyoMesDetalleKey(obj), roundValor(proporcionSalarioMes));
                            } else {
                                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_ARL" + anyoMesDetalleKey(obj), roundValor(valorResultante));
                            }
                        }
                    }
                    if ("21".equals(obj.getNominaDetalle().getTipoCotizante()) || "20".equals(obj.getNominaDetalle().getTipoCotizante())
                            || "19".equals(obj.getNominaDetalle().getTipoCotizante())) {
                        // SAlario Minimo buscado en Cache
                        cobParamGeneral = (CobParamGeneral) cacheService.get(CacheService.REGION_COBPARAMGENERAL, "SMMLV" + obj.getNominaDetalle().getAno().toString());
                        rstSmml = new BigDecimal("0");
                        if (cobParamGeneral != null && cobParamGeneral.getValor() != 0) {
                            rstSmml = new BigDecimal(cobParamGeneral.getValor().toString());
                        }
                        BigDecimal diaSmml = rstSmml.divide(new BigDecimal("30"), 2, RoundingMode.HALF_UP);
                        BigDecimal rstMulDia = mulValorReglas(diaSmml, infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#DIAS_COT_RPROF" + anyoMesDetalleKey(obj)));
                        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_ARL" + anyoMesDetalleKey(obj), roundValor(rstMulDia));
                    }
                }

            }

        }
     
        // Regla #65 <TARIFA ARL>
        if (pilaDepurada == null) {
            // Si no se encuentra en pila depurada se trae la tarifa más alta del año-mes-nit (Pendiente definición de UGPP)
            // Ene.02.2017. Se toma el campo TARIFA_MAXIMA de la tabla <LIQ_PILA_DEPURADA> pero esta tarifa NO es la que realmente se define en la regla
            //float valorTarifaCentroTrabajo = gestorProgramaDao.obtenerTarifaCentroTrabajoPilaDepuradaNominaDetalle(obj.getNomina(), obj.getNominaDetalle());

            Float valorMaximoTarifaCentroTrabajo = gestorProgramaDao.obtenerMaximaTarifaCentroTrabajoPilaDepuradaNominaDetalle(obj.getNomina(), obj.getNominaDetalle());

            /*
            if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("98765") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 1)
            {
                    System.out.println("::ANDRES60:: valorMaximoTarifaCentroTrabajo: " + valorMaximoTarifaCentroTrabajo);
            }
            */
            
            if (valorMaximoTarifaCentroTrabajo.compareTo(0f) == 0) {
                
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_ARL" + anyoMesDetalleKey(obj), gestorProgramaDao.obtenerMaximaTarifaAportante(obj.getNomina(), obj.getNominaDetalle().getAno().toString()));
                //System.out.println("::ANDRES0:: anyoMesDetalleKey: " + anyoMesDetalleKey(obj));
                //System.out.println("::ANDRES1:: " + obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_ARL se setea al maximo obtenerMaximaTarifaAportante: " + gestorProgramaDao.obtenerMaximaTarifaAportante(obj.getNomina(), obj.getNominaDetalle().getAno().toString()));
            
                /*
                if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("98765") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 1)
                {
                    System.out.println("::ANDRES61:: valorMaximoTarifaCentroTrabajo: 0");
                }
                 */
            
            } else {
                
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_ARL" + anyoMesDetalleKey(obj), valorMaximoTarifaCentroTrabajo);
                
                /*
                if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("98765") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 1)
                {
                    System.out.println("::ANDRES62:: valorMaximoTarifaCentroTrabajo: " + valorMaximoTarifaCentroTrabajo);
                }
                */

                //System.out.println("::ANDRES0:: anyoMesDetalleKey: " + anyoMesDetalleKey(obj));
                //System.out.println("::ANDRES2:: " + obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_ARL se setea valorMaximoTarifaCentroTrabajo: " + valorMaximoTarifaCentroTrabajo);
            }

        } else {

            Float valorTarifaCentroTrabajo = gestorProgramaDao.obtenerTarifaCentroTrabajoPilaDepuradaNominaDetalle(obj.getNomina(), obj.getNominaDetalle());

            /*
            if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("98765") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 1)
            {
                System.out.println("::ANDRES63:: valorTarifaCentroTrabajo: " + valorTarifaCentroTrabajo);
            }
            */
            
            
            if (valorTarifaCentroTrabajo.compareTo(0f) == 0) 
            {    //toma la tarifa mas alta

                Float valorMaximoTarifaCentroTrabajo = gestorProgramaDao.obtenerMaximaTarifaCentroTrabajoPilaDepuradaNominaDetalle(obj.getNomina(), obj.getNominaDetalle());

                /*
                if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("98765") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 1)
                {
                    System.out.println("::ANDRES64:: valorMaximoTarifaCentroTrabajo: " + valorMaximoTarifaCentroTrabajo);
                }          
                */
                
                
                if (valorMaximoTarifaCentroTrabajo.compareTo(0f) == 0) {
                    
                    if(pilaDepurada.getTarifaMaxima() != null)
                    {
                        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_ARL" + anyoMesDetalleKey(obj), Double.valueOf(pilaDepurada.getTarifaMaxima()));
                        //System.out.println("::ANDRES0:: getTarifaMaxima: " + Double.valueOf(pilaDepurada.getTarifaMaxima()));
                        
                        /*
                        if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("98765") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 1)
                        {
                            System.out.println("::ANDRES65:: pilaDepurada.getTarifaMaxima: " + Double.valueOf(pilaDepurada.getTarifaMaxima()));
                        }   
                        */  
                    }
                    else
                    {
                        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_ARL" + anyoMesDetalleKey(obj), pilaDepurada.getTarifaMaxima());
                        //System.out.println("::ANDRES0:: getTarifaMaxima: " + pilaDepurada.getTarifaMaxima());
                        
                         /*
                        if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("98765") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 1)
                        {
                            System.out.println("::ANDRES66:: pilaDepurada.getTarifaMaxima: " + pilaDepurada.getTarifaMaxima());
                        }
                        */ 
                        
                    }
                       
                    //System.out.println("::ANDRES0:: anyoMesDetalleKey: " + anyoMesDetalleKey(obj));
                    //System.out.println("::ANDRES3:: " + obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_ARL getTarifaMaxima "  + pilaDepurada.getTarifaMaxima() + " piladepurada " + pilaDepurada.getId() + " valorMaximoTarifaCentroTrabajo: " + valorMaximoTarifaCentroTrabajo);
                } else {
                    
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_ARL" + anyoMesDetalleKey(obj), valorMaximoTarifaCentroTrabajo);
                    
                    /*
                    if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("98765") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 1)
                    {
                        System.out.println("::ANDRES67:: valorMaximoTarifaCentroTrabajo: " + valorMaximoTarifaCentroTrabajo);
                    }
                    */ 
                    
                    //System.out.println("::ANDRES0:: anyoMesDetalleKey: " + anyoMesDetalleKey(obj));
                    //System.out.println("::ANDRES4:: " + obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_ARL valorMaximoTarifaCentroTrabajo "  + valorMaximoTarifaCentroTrabajo + " piladepurada " + pilaDepurada.getId() + " valorMaximoTarifaCentroTrabajo: " + valorMaximoTarifaCentroTrabajo);
                }
            } else {
                
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_ARL" + anyoMesDetalleKey(obj), valorTarifaCentroTrabajo);
                
                /*
                if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("98765") && obj.getNominaDetalle().getAno().intValue() == 2017 && obj.getNominaDetalle().getMes().intValue() == 1)
                {
                    System.out.println("::ANDRES68:: valorTarifaCentroTrabajo: " + valorTarifaCentroTrabajo);
                }
                */
                
                //System.out.println("::ANDRES0:: anyoMesDetalleKey: " + anyoMesDetalleKey(obj));
                //System.out.println("::ANDRES5:: " + obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_ARL valorTarifaCentroTrabajo "  + valorTarifaCentroTrabajo + " piladepurada " + pilaDepurada.getId() + " valorTarifaCentroTrabajo: " + valorTarifaCentroTrabajo);
            }
        }

        
        // Regla #66 <COTIZACION OBLIGATORIA ARL>
        BigDecimal ibc_arl = new BigDecimal(String.valueOf(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_ARL" + anyoMesDetalleKey(obj))));
        
        BigDecimal tarifa_arl = new BigDecimal("0");
        
        
        if(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_ARL" + anyoMesDetalleKey(obj)) != null)
            tarifa_arl = new BigDecimal(String.valueOf(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_ARL" + anyoMesDetalleKey(obj))));

        rst = mulValorReglas(ibc_arl, tarifa_arl);
        //rst = rst.divide(new BigDecimal("100"),2,RoundingMode.HALF_UP);
        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_OBL_ARL" + anyoMesDetalleKey(obj), roundValor100(rst));

        // Regla #67 <DIAS COTIZADOS PILA ARL>
        if (pilaDepurada != null) {
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#DIAS_COTIZ_PILA_ARL" + anyoMesDetalleKey(obj), pilaDepurada.getDiasCotRprof());
        }

        // Regla #68 <IBC PILA ARL>
        if (pilaDepurada != null) {
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_PILA_ARL" + anyoMesDetalleKey(obj), pilaDepurada.getIbcRprof());
        }

        // Regla #69 <TARIFA PILA ARL>
        if (pilaDepurada == null) {
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_PILA_ARL" + anyoMesDetalleKey(obj), new BigDecimal("0"));
        } else {
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_PILA_ARL" + anyoMesDetalleKey(obj), pilaDepurada.getTarifaCentroTrabajo());
        }

        // Regla #70 <COTIZACION PAGADA PILA ARL>
        if (pilaDepurada != null) {
            
            if(StringUtils.isNotBlank(obj.getNominaDetalle().getCargueManualPilaArl().toString()) && convertValorRegla(obj.getNominaDetalle().getCargueManualPilaArl().toString()).intValue() > 0)
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_PAGADA_PILA_ARL" + anyoMesDetalleKey(obj), obj.getNominaDetalle().getCargueManualPilaArl().toString());
            else
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_PAGADA_PILA_ARL" + anyoMesDetalleKey(obj), pilaDepurada.getCotObligatoriaArp());
        }

        

        // Regla #71 <AJUSTE ARL>
        rst = minusValorReglas(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_OBL_ARL" + anyoMesDetalleKey(obj)),
                infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_PAGADA_PILA_ARL" + anyoMesDetalleKey(obj)));

        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#AJUSTE_ARL" + anyoMesDetalleKey(obj), rst);

        // Regla #72 <CONCEPTO AJUSTE ARL>
        rst = convertValorRegla(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#AJUSTE_ARL" + anyoMesDetalleKey(obj)));

        BigDecimal cotPagPilaARL = convertValorRegla(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_PAGADA_PILA_ARL" + anyoMesDetalleKey(obj)));

        if (cotPagPilaARL == null) {
            cotPagPilaARL = BigDecimal.ZERO;
        }

        /*
                if(obj.getNominaDetalle().getNumeroIdentificacionActual().equals("14965368"))
                {
                    System.out.println("::ANDRES07:: cotPagPilaARL: " + cotPagPilaARL); 
                    System.out.println("::ANDRES08:: COD_ADM_ARL: " + infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COD_ADM_ARL" + anyoMesDetalleKey(obj))); 
                }
        */
        
        
        
        
        
        if("X".equals(obj.getNominaDetalle().getOmisionArl()))
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CONCEPTO_AJUSTE_ARL" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.OMISO_DESC);
        else
        {
            if (rst.intValue() >= 1000) {
                if (infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COD_ADM_ARL" + anyoMesDetalleKey(obj)) == null && cotPagPilaARL.compareTo(BigDecimal.ZERO) == 0) {
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CONCEPTO_AJUSTE_ARL" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.OMISO_DESC);
                    //System.out.println("::ANDRES09:: seteo: " + ConstantesGestorPrograma.OMISO_DESC);
                } else if (infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COD_ADM_ARL" + anyoMesDetalleKey(obj)) != null && cotPagPilaARL.compareTo(BigDecimal.ZERO) == 0) {
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CONCEPTO_AJUSTE_ARL" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.MORA_DESC);
                    //System.out.println("::ANDRES09:: seteo: " + ConstantesGestorPrograma.MORA_DESC);
                } else if (cotPagPilaARL.compareTo(BigDecimal.ZERO) != 0) {
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CONCEPTO_AJUSTE_ARL" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.INEXACTO_DESC);
                    //System.out.println("::ANDRES09:: seteo: " + ConstantesGestorPrograma.INEXACTO_DESC);
                }
            }
        }
        
        
        
        
        
        // Regla #73 <TIPO INCUMPLIMIENTO ARL>
        rst = convertValorRegla(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#AJUSTE_ARL" + anyoMesDetalleKey(obj)));
        
        if (rst.intValue() >= 1000) 
        {
            String conceAjuArl = (String) infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CONCEPTO_AJUSTE_ARL" + anyoMesDetalleKey(obj));
            if (null != conceAjuArl) 
            {
                switch (conceAjuArl) {
                    case ConstantesGestorPrograma.OMISO_DESC:
                        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TIPO_INCUMPLIMIENTO_ARL" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.OMISO_NOMBRE);
                        break;
                    case ConstantesGestorPrograma.MORA_DESC:
                        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TIPO_INCUMPLIMIENTO_ARL" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.MORA_NOMBRE);
                        break;
                    case ConstantesGestorPrograma.INEXACTO_DESC:
                        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TIPO_INCUMPLIMIENTO_ARL" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.INEXACTO_NOMBRE);
                        break;
                    default:
                        break;
                }
            } 
        }

        
        String valorAnteriorCCF = (String)infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COD_ADM_CCF" + anyoMesDetalleKey(obj));
        //System.out.println("::ANDRES00:: valorAnterior: " + valorAnteriorCCF + " MES: " + anyoMesDetalleKey(obj) + " en blanco: " + StringUtils.isNotBlank(valorAnteriorCCF));
        
        // Regla #75 <CODIGO ADMINISTRADORA CCF>
        // Regla #76 <NOMBRE CORTO ADMINISTRADORA CCF>
        if(StringUtils.isBlank(valorAnteriorCCF))
        {
            if (pilaDepurada != null && StringUtils.isNotBlank(pilaDepurada.getCodigoCCF())) 
            {
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COD_ADM_CCF" + anyoMesDetalleKey(obj), pilaDepurada.getCodigoCCF());
                //System.out.println("::ANDRES20:: pilaDepurada: " + pilaDepurada.getId() + " MES: " + anyoMesDetalleKey(obj) + " CCF: " + pilaDepurada.getCodigoCCF());

                String nomAdmPension = LST_ADMINISTRADORA_PILA.get(pilaDepurada.getCodigoCCF());
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#NOM_ADM_CCF" + anyoMesDetalleKey(obj), nomAdmPension);

            }
            else
            {
                String codObsCCF = gestorProgramaDao.observacionCCF(obj.getNominaDetalle());
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COD_ADM_CCF" + anyoMesDetalleKey(obj), codObsCCF);
                //System.out.println("::ANDRES21:: getNominaDetalle: " + obj.getNominaDetalle().getId() + " MES: " + anyoMesDetalleKey(obj) + " CCF: " + codObsCCF);

                String nomAdmCCF = LST_ADMINISTRADORA_PILA.get(codObsCCF);
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#NOM_ADM_CCF" + anyoMesDetalleKey(obj), nomAdmCCF);

            }
        }     

        
        // Regla #77 <IBC CCF>
        //Map<String, String> mapMallaval = gestorProgramaDao.obtenerMallaVal(obj.getNominaDetalle());
        if (mapMallaval != null && StringUtils.contains("NO", mapMallaval.get("TIENE_CCF"))) {
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CCF" + anyoMesDetalleKey(obj), new BigDecimal("0"));
        } else {
            tmp2 = gestorProgramaDao.sumaValorLiqConceptoContableIBC_CCF(obj.getNominaDetalle(),"0","0","0","4","0","0");
            BigDecimal resultado = tmp2;
            if ("X".equals(obj.getNominaDetalle().getSalarioIntegral())) {
                BigDecimal por70 = tmp2.multiply(new BigDecimal("70"));
                por70 = por70.divide(new BigDecimal("100"));
                resultado = por70;
            }
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CCF" + anyoMesDetalleKey(obj), roundValor(resultado));
        }

        // Regla #78 <TARIFA CCF>
        rst = gestorProgramaDao.tarifaCCF(obj.getNomina(), obj.getNominaDetalle());
        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_CCF" + anyoMesDetalleKey(obj), rst);

        if (null != obj.getNominaDetalle().getCondEspEmp()) 
        {
            switch (obj.getNominaDetalle().getCondEspEmp()) 
            {
                case "LEY 590/2000 AÑO 1":
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_CCF" + anyoMesDetalleKey(obj), new BigDecimal("1"));
                    break;
                case "LEY 590/2000 AÑO 2":
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_CCF" + anyoMesDetalleKey(obj), new BigDecimal("2"));
                    break;
                case "LEY 590/2000 AÑO 3":
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_CCF" + anyoMesDetalleKey(obj), new BigDecimal("3"));
                    break;
                case "LEY 1429 Col AÑO 1,2":
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_CCF" + anyoMesDetalleKey(obj), new BigDecimal("0"));
                    break;
                case "LEY 1429 Col AÑO 3":
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_CCF" + anyoMesDetalleKey(obj), new BigDecimal("1"));
                    break;
                case "LEY 1429 Col AÑO 4":
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_CCF" + anyoMesDetalleKey(obj), new BigDecimal("2"));
                    break;
                case "LEY 1429 Col AÑO 5":
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_CCF" + anyoMesDetalleKey(obj), new BigDecimal("3"));
                    break;
                case "LEY 1429 AGV AÑO 1-8":
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_CCF" + anyoMesDetalleKey(obj), new BigDecimal("0"));
                    break;
                case "LEY 1429 AGV AÑO 9":
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_CCF" + anyoMesDetalleKey(obj), new BigDecimal("2"));
                    break;
                case "LEY 1429 AGV AÑO 10":
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_CCF" + anyoMesDetalleKey(obj), new BigDecimal("3"));
                    break;
                default:
                    break;
            }
        }
        
        
        // Regla #79 <COTIZACION OBLIGATORIA CCF>
        rst = mulValorReglas(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CCF" + anyoMesDetalleKey(obj)),
                infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_CCF" + anyoMesDetalleKey(obj)));
        rst = rst.divide(new BigDecimal("100"));
        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_OBL_CCF" + anyoMesDetalleKey(obj), roundValor100(rst));

        // Regla #80 <DIAS COTIZADOS PILA CCF>
        if (pilaDepurada == null) {
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#DIAS_COTI_PILA_CCF" + anyoMesDetalleKey(obj), new BigDecimal("0"));
        } else {
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#DIAS_COTI_PILA_CCF" + anyoMesDetalleKey(obj), pilaDepurada.getDiasCotCcf());
        }

        // Regla #81 <IBC PILA CCF>
        if (pilaDepurada == null) {
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_PILA_CCF" + anyoMesDetalleKey(obj), new BigDecimal("0"));
        } else {
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_PILA_CCF" + anyoMesDetalleKey(obj), pilaDepurada.getIbcCcf());
        }

        // Regla #82 <TARIFA PILA CCF>
        if (pilaDepurada == null) {
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_PILA_CCF" + anyoMesDetalleKey(obj), new BigDecimal("0"));
        } else {
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_PILA_CCF" + anyoMesDetalleKey(obj), pilaDepurada.getTarifaAportesCcf());
        }

        // Regla #83 <COTIZACION PAGADA PILA CCF>
        if (pilaDepurada == null) {
            
             if(StringUtils.isNotBlank(obj.getNominaDetalle().getCargueManualPilaCcf().toString()) && convertValorRegla(obj.getNominaDetalle().getCargueManualPilaCcf().toString()).intValue() > 0)
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_PAGADA_PILA_CCF" + anyoMesDetalleKey(obj), obj.getNominaDetalle().getCargueManualPilaCcf().toString());
            else
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_PAGADA_PILA_CCF" + anyoMesDetalleKey(obj), new BigDecimal("0"));
        
        } else {
            
            if(StringUtils.isNotBlank(obj.getNominaDetalle().getCargueManualPilaCcf().toString()) && convertValorRegla(obj.getNominaDetalle().getCargueManualPilaCcf().toString()).intValue() > 0)
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_PAGADA_PILA_CCF" + anyoMesDetalleKey(obj), obj.getNominaDetalle().getCargueManualPilaCcf().toString());
            else
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_PAGADA_PILA_CCF" + anyoMesDetalleKey(obj), pilaDepurada.getValorAportesCcfIbcTarifa());
        }

        // Regla #84 <AJUSTE CCF>
        rst = minusValorReglas(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_OBL_CCF" + anyoMesDetalleKey(obj)),
                infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_PAGADA_PILA_CCF" + anyoMesDetalleKey(obj)));
        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#AJUSTE_CCF" + anyoMesDetalleKey(obj), rst);

        // Regla #85 <CONCEPTO AJUSTE CCF>
        rst = convertValorRegla(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#AJUSTE_CCF" + anyoMesDetalleKey(obj)));

        
        
        
        
        if("X".equals(obj.getNominaDetalle().getOmisionCcf()))
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CONCEPTO_AJUSTE_CCF" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.OMISO_DESC);
        else
        {
            if (rst.intValue() >= 1000) {

                BigDecimal concepAjuste = convertValorRegla(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_PAGADA_PILA_CCF" + anyoMesDetalleKey(obj)));

                if (concepAjuste == null) {
                    concepAjuste = BigDecimal.ZERO;
                }

                if (infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COD_ADM_CCF" + anyoMesDetalleKey(obj)) == null && concepAjuste.compareTo(BigDecimal.ZERO) == 0) {
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CONCEPTO_AJUSTE_CCF" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.OMISO_DESC);
                } else if (infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COD_ADM_CCF" + anyoMesDetalleKey(obj)) != null && concepAjuste.compareTo(BigDecimal.ZERO) == 0) {
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CONCEPTO_AJUSTE_CCF" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.MORA_DESC);
                } else if (concepAjuste.compareTo(BigDecimal.ZERO) != 0) {
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CONCEPTO_AJUSTE_CCF" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.INEXACTO_DESC);
                }
            }
        }
        
        
        
        //System.out.println("::ANDRES98:: procesarReglasNoFormula pilaDepurada: ");
        

        // Regla #86 <TIPO DE INCUMPLIMIENTO CCF>
        rst = convertValorRegla(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#AJUSTE_CCF" + anyoMesDetalleKey(obj)));

        if (rst.intValue() >= 1000) {

            String conceAjuCcf = (String) infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CONCEPTO_AJUSTE_CCF" + anyoMesDetalleKey(obj));

            if (null != conceAjuCcf) {
                switch (conceAjuCcf) {
                    case ConstantesGestorPrograma.OMISO_DESC:
                        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TIPO_INCUMPLIMIENTO_CCF" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.OMISO_NOMBRE);
                        break;
                    case ConstantesGestorPrograma.MORA_DESC:
                        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TIPO_INCUMPLIMIENTO_CCF" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.MORA_NOMBRE);
                        break;
                    case ConstantesGestorPrograma.INEXACTO_DESC:
                        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TIPO_INCUMPLIMIENTO_CCF" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.INEXACTO_NOMBRE);
                        break;
                    default:
                        break;
                }
            } 
            
        }
        
        
        

        // Regla #88 <IBC SENA>
        //mapMallaval = gestorProgramaDao.obtenerMallaVal(obj.getNominaDetalle());

        // MES Y AÑO DE NOMINA

        
        if(obj.getNominaDetalle().getMes().intValue() > 9)
            mesNomina =  obj.getNominaDetalle().getMes().toString();
        else
            mesNomina =  "0" + obj.getNominaDetalle().getMes();
                         


        if (mapMallaval != null && StringUtils.contains("NO", mapMallaval.get("TIENE_SENA"))) {
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_SENA" + anyoMesDetalleKey(obj), new BigDecimal("0"));
            //System.out.println("::ANDRES90:: mapMallaval IBC_SENA: " + 0);
        } 
        else 
        {
            
            if(!obj.getNominaDetalle().getTipoCotizante().equals("31"))
            {
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_SENA" + anyoMesDetalleKey(obj),
                infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CCF" + anyoMesDetalleKey(obj)));
                //System.out.println("::ANDRES91:: coloca el mismo IBC_CCF : " + infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CCF" + anyoMesDetalleKey(obj)));
            
            }
            else
            {
                tmp2 = gestorProgramaDao.sumaValorLiqConceptoContableIBC(obj.getNominaDetalle(),"0","0","0","0","5","0");
                //sumNOTpIncapacidadVacaciones = gestorProgramaDao.sumaValorLiqConceptoContableIbcPagosNomSaludNoTpIncapacidadVacaciones(obj.getNomina(), obj.getNominaDetalle(), "5");
                //sumNOTpIncapacidad = sumNOTpIncapacidad.add(sumNOTpIncapacidadVacaciones);
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_SENA" + anyoMesDetalleKey(obj),tmp2);
                //System.out.println("::ANDRES92:: sumaValorLiqConceptoContableIBC : " + tmp2);
            }

            
            // tipoIde = gestorProgramaDao.tipoIdentificacionAportante(obj.getNominaDetalle());
            BigDecimal valorCalculado = convertValorRegla(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TOTAL_DEVENGADO" + anyoMesDetalleKey(obj)));
            // SAlario Minimo buscado en Cache
            cobParamGeneral = (CobParamGeneral) cacheService.get(CacheService.REGION_COBPARAMGENERAL, "SMMLV" + obj.getNominaDetalle().getAno().toString());
            rstSmml = new BigDecimal("0");
            if (cobParamGeneral != null && cobParamGeneral.getValor() != 0) {
                rstSmml = new BigDecimal(cobParamGeneral.getValor().toString());
            }
            
            
            
            // CASO DEL CREE
            //1- Si el campo “SUJETO PASIVO DEL IMPUESTO SOBRE LA RENTA PARA LA EQUIDAD CREE” está marcado con ""SI"" y  el campo 
            //""TIPO DOCUMENTO APORTANTE"" es igual a ""NI"", se registrará cero (0) en el IBC SENA así:
            //a- entre mayo 2013  y  diciembre 2016, a trabajadores que ""devenguen"", individualmente considerados, hasta diez (10) SMLMV (Ley 1607/2012).
            //b- a partir de enero 2017 a trabajadores que ""devenguen"", individualmente considerados, menos de diez (10) SMLMV (Ley 1819/2016).
            if ("SI".equals(obj.getNominaDetalle().getIdaportante().getSujetoPasivoImpuestoCree()))
            {  
                
                //System.out.println("::ANDRES70:: getSujetoPasivoImpuestoCree igual a SI");
                
                
                if(tipoIdentificacionAportante.intValue() == 2)
                {
                    
                    try 
                    {
                        //entre mayo 2013  y  diciembre 2016
                        fechaCree1 = formateador.parse("30/04/2013");
                        fechaCree2 = formateador.parse("01/01/2017");
                        
                        if(fechaNomina != null && fechaNomina.after(fechaCree1) && fechaNomina.before(fechaCree2) && valorCalculado.doubleValue() <= smml10.doubleValue())
                        {
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_SENA" + anyoMesDetalleKey(obj), new BigDecimal("0"));
                            //System.out.println("::ANDRES93:: getSujetoPasivoImpuestoCree : " + 0);
                        }

                        //a partir de enero 2017 
                        fechaCree1 = formateador.parse("31/12/2016");
                        
                        
                        if (fechaNomina != null && fechaNomina.after(fechaCree1) && valorCalculado.doubleValue() < smml10.doubleValue())
                        {
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_SENA" + anyoMesDetalleKey(obj), new BigDecimal("0"));
                            //System.out.println("::ANDRES94:: valorCalculado : " + 0);
                        }
                        
                    } catch (ParseException ex) {
                        Logger.getLogger(NominaModoEjecucion.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
                
                //2- si el campo “SUJETO PASIVO DEL IMPUESTO SOBRE LA RENTA PARA LA EQUIDAD CREE” está marcado con ""SI"", 
                //el campo ""TIPO DOCUMENTO APORTANTE"" es diferente a ""NI"",  el periodo fiscalizado es posterior a abril de 2013, 
                //y existe mas de un trabajador en el mes de la nomina que se esta fiscalizando,  el IBC SENA será igual a cero (0) 
                //para los trabajadores que ""devenguen"", individualmente considerados, menos de diez (10) salarios mínimos mensuales legales vigentes.
                if(tipoIdentificacionAportante.intValue() != 2)
                {
                    
                    try {
                        
                        
                        //System.out.println("::ANDRES72:: tipoIdentificacionAportante diferente a 2");
                        
                        //BigDecimal cantidadEmp = gestorProgramaDao.obtenerEmpleadoPeriodo(obj.getNominaDetalle());
                        
                        //el periodo fiscalizado es posterior a abril de 2013
                        fechaCree1 = formateador.parse("30/04/2013");

                        
                        if (fechaNomina != null && fechaNomina.after(fechaCree1) && cantidadEmp.compareTo(BigDecimal.ONE) > 1 && valorCalculado.doubleValue() < smml10.doubleValue())
                        {
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_SENA" + anyoMesDetalleKey(obj), new BigDecimal("0"));
                            //System.out.println("::ANDRES95:: tipoIdentificacionAportante : " + 0);
                        }
                        
                        
                    } catch (ParseException ex) {
                        Logger.getLogger(NominaModoEjecucion.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }
        }
        
        

        // Regla #89 <TARIFA SENA>
        rst = gestorProgramaDao.tarifaSena(obj.getNomina(), obj.getNominaDetalle());
        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_SENA" + anyoMesDetalleKey(obj), rst);
        if ("1".equals(obj.getNominaDetalle().getIdaportante().getNaturalezaJuridica()) && "X".equals(obj.getNominaDetalle().getIdaportante().getAportaEsapYMen())) {
            BigDecimal rsttf = new BigDecimal("1");
            rsttf = rsttf.divide(new BigDecimal("2"));
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_SENA" + anyoMesDetalleKey(obj), rsttf);
        } else {
            if ("2".equals(obj.getNominaDetalle().getIdaportante().getNaturalezaJuridica())) {
                BigDecimal rst05 = new BigDecimal("1");
                rst05 = rst05.divide(new BigDecimal("2"));
                BigDecimal rst15 = new BigDecimal("3");
                rst15 = rst15.divide(new BigDecimal("2"));
                if (null != obj.getNominaDetalle().getCondEspEmp()) {
                    switch (obj.getNominaDetalle().getCondEspEmp()) {
                        case "LEY 590/2000 AÑO 1":
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_SENA" + anyoMesDetalleKey(obj), rst05);
                            break;
                        case "LEY 590/2000 AÑO 2":
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_SENA" + anyoMesDetalleKey(obj), new BigDecimal("1"));
                            break;
                        case "LEY 590/2000 AÑO 3":
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_SENA" + anyoMesDetalleKey(obj), rst15);
                            break;
                        case "LEY 1429 Col AÑO 1,2":
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_SENA" + anyoMesDetalleKey(obj), new BigDecimal("0"));
                            break;
                        case "LEY 1429 Col AÑO 3":
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_SENA" + anyoMesDetalleKey(obj), rst05);
                            break;
                        case "LEY 1429 Col AÑO 4":
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_SENA" + anyoMesDetalleKey(obj), new BigDecimal("1"));
                            break;
                        case "LEY 1429 Col AÑO 5":
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_SENA" + anyoMesDetalleKey(obj), rst15);
                            break;
                        case "LEY 1429 AGV AÑO 1-8":
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_SENA" + anyoMesDetalleKey(obj), new BigDecimal("0"));
                            break;
                        case "LEY 1429 AGV AÑO 9":
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_SENA" + anyoMesDetalleKey(obj), new BigDecimal("1"));
                            break;
                        case "LEY 1429 AGV AÑO 10":
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_SENA" + anyoMesDetalleKey(obj), rst15);
                            break;
                        case "Soc.declaradas ZF.Art20 Ley 1607":
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_SENA" + anyoMesDetalleKey(obj), new BigDecimal("2"));
                            break;
                        case "Excepcion SENA Art.181,Ley 223/95":
                            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_SENA" + anyoMesDetalleKey(obj), new BigDecimal("0"));
                            break;
                        default:
                            break;
                    }
                }
            }
        }

        // Regla #90 <COTIZACION OBLIGATORIA SENA>
        rst = mulValorReglas(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_SENA" + anyoMesDetalleKey(obj)),
                infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_SENA" + anyoMesDetalleKey(obj)));
        rst = rst.divide(new BigDecimal("100"));
        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_OBL_SENA" + anyoMesDetalleKey(obj), roundValor100(rst));

        // Regla #91 <TARIFA PILA SENA>
        if (pilaDepurada != null) {
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_PILA_SENA" + anyoMesDetalleKey(obj), pilaDepurada.getTarifaAportesSena());
        }

        
        
        //System.out.println("::ANDRES01:: getNumeroIdentificacion: " + obj.getNominaDetalle().getNumeroIdentificacionActual()); 
        //System.out.println("::ANDRES01:: anyoMesDetalleKey: " + anyoMesDetalleKey(obj));    
        //System.out.println("::ANDRES01:: anyo DetalleKey: " +  obj.getNominaDetalle().getAno().toString());
        //System.out.println("::ANDRES01:: mes DetalleKey: " +  obj.getNominaDetalle().getMes().toString());
        //System.out.println("::ANDRES01:: getNit: " +  obj.getNomina().getNit().toString());
             

        // Regla #92 <COTIZACION PAGADA PILA SENA>
        //Se debe colocar valor de la cotización de SENA registrado en el PILA DEPURADA de 
        //acuerdo a la consulta por número DOCUMENTO APORTANTE, 
        //número NUMERO DOCUMENTO ACTUAL DEL COTIZANTE, mes y año de la fiscalización.
        
        if(StringUtils.isNotBlank(obj.getNominaDetalle().getCargueManualPilaSena().toString()) && convertValorRegla(obj.getNominaDetalle().getCargueManualPilaSena().toString()).intValue() > 0)
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_PAGADA_PILA_SENA" + anyoMesDetalleKey(obj), obj.getNominaDetalle().getCargueManualPilaSena().toString());
        else
        {
            if (pilaDepurada != null && pilaDepurada.getValorAportesParafiscalesSena() != null) 
            {
                //System.out.println("::ANDRES02:: pilaDepurada: " + pilaDepurada.getId());
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_PAGADA_PILA_SENA" + anyoMesDetalleKey(obj), pilaDepurada.getValorAportesParafiscalesSena());
            }
            else
            {
                    PilaDepurada pilaDepuradaRealizoAportes = gestorProgramaDao.obtegerPilaDepuradaNominaDetalleCotizanteRealizoAportes(obj.getNomina(), obj.getNominaDetalle());

                    //En caso de no encontrar información, se debe colocar valor de la cotización 
                    //de SENA registrado en el PILA DEPURADA de acuerdo a la consulta por número 
                    //DOCUMENTO APORTANTE, número DOCUMENTO CON EL QUE REALIZO APORTES DEL COTIZANTE, mes y año de la fiscalización"
                    if(pilaDepuradaRealizoAportes != null && pilaDepuradaRealizoAportes.getValorAportesParafiscalesSena() != null)
                    {
                        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_PAGADA_PILA_SENA" + anyoMesDetalleKey(obj), pilaDepuradaRealizoAportes.getValorAportesParafiscalesSena());
                        //System.out.println("::ANDRES06:: getValorAportesParafiscalesSena: " + pilaDepuradaRealizoAportes.getValorAportesParafiscalesSena());
                    }

            }
        }
        
        
        //System.out.println("::ANDRES99:: procesarReglasNoFormula pilaDepurada: ");
        
        
        // Regla #93 <AJUSTE SENA>
        rst = minusValorReglas(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_OBL_SENA" + anyoMesDetalleKey(obj)),
                infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_PAGADA_PILA_SENA" + anyoMesDetalleKey(obj)));
        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#AJUSTE_SENA" + anyoMesDetalleKey(obj), rst);

        
        // Regla #94 <CONCEPTO AJUSTE SENA>
        rst = convertValorRegla(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#AJUSTE_SENA" + anyoMesDetalleKey(obj)));

        BigDecimal conceAjuSENA = convertValorRegla(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_PAGADA_PILA_SENA" + anyoMesDetalleKey(obj)));

        if (conceAjuSENA == null) {
            conceAjuSENA = BigDecimal.ZERO;
        }

        if (rst.intValue() >= 1000) {

            if (conceAjuSENA.compareTo(BigDecimal.ZERO) == 0) {
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CONCEPTO_AJUSTE_SENA" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.MORA_DESC);
            } else {
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CONCEPTO_AJUSTE_SENA" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.INEXACTO_DESC);
            }

        }

        // Regla #95 <TIPO DE INCUMPLIMIENTO SENA>
        rst = convertValorRegla(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#AJUSTE_SENA" + anyoMesDetalleKey(obj)));
        if (rst.intValue() >= 1000) {
            String conceAjuSena = (String) infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CONCEPTO_AJUSTE_SENA" + anyoMesDetalleKey(obj));
            if (null != conceAjuSena) {
                switch (conceAjuSena) {
                    case ConstantesGestorPrograma.OMISO_DESC:
                        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TIPO_INCUMPLIMIENTO_SENA" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.OMISO_NOMBRE);
                        break;
                    case ConstantesGestorPrograma.MORA_DESC:
                        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TIPO_INCUMPLIMIENTO_SENA" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.MORA_NOMBRE);
                        break;
                    case ConstantesGestorPrograma.INEXACTO_DESC:
                        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TIPO_INCUMPLIMIENTO_SENA" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.INEXACTO_NOMBRE);
                        break;
                    default:
                        break;
                }   
            }
        }

        // Regla #97 <IBC ICBF>
        //mapMallaval = gestorProgramaDao.obtenerMallaVal(obj.getNominaDetalle());
        if (mapMallaval != null && StringUtils.contains("NO", mapMallaval.get("TIENE_ICBF"))) {
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_ICBF" + anyoMesDetalleKey(obj), new BigDecimal("0"));
        } else {
            
            
            if(!obj.getNominaDetalle().getTipoCotizante().equals("31"))
            {
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_ICBF" + anyoMesDetalleKey(obj),
                    infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_CCF" + anyoMesDetalleKey(obj)));
            }
            else
            {
                tmp2 = gestorProgramaDao.sumaValorLiqConceptoContableIBC(obj.getNominaDetalle(),"0","0","0","0","0","6");
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_ICBF" + anyoMesDetalleKey(obj),tmp2);
            }
            
           
            // tipoIde = gestorProgramaDao.tipoIdentificacionAportante(obj.getNominaDetalle());
            if ("SI".equals(obj.getNominaDetalle().getIdaportante().getSujetoPasivoImpuestoCree())) 
            {
                // SAlario Minimo buscado en Cache
                cobParamGeneral = (CobParamGeneral) cacheService.get(CacheService.REGION_COBPARAMGENERAL, "SMMLV" + obj.getNominaDetalle().getAno().toString());
                rstSmml = new BigDecimal("0");
                if (cobParamGeneral != null && cobParamGeneral.getValor() != 0) {
                    rstSmml = new BigDecimal(cobParamGeneral.getValor().toString());
                }
                
                smml10 = rstSmml.multiply(new BigDecimal("10"));
                BigDecimal valorCalculado = convertValorRegla(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TOTAL_DEVENGADO" + anyoMesDetalleKey(obj)));

                
                   // CASO DEL CREE
                   //1- Si el campo “SUJETO PASIVO DEL IMPUESTO SOBRE LA RENTA PARA LA EQUIDAD CREE” está marcado con ""SI"" y  el campo 
                   //""TIPO DOCUMENTO APORTANTE"" es igual a ""NI"", se registrará cero (0) en el IBC ICBF así:
                   //a- entre mayo 2013  y  diciembre 2016, a trabajadores que ""devenguen"", individualmente considerados, hasta diez (10) SMLMV (Ley 1607/2012)
                   //b- a partir de enero 2017 a trabajadores que ""devenguen"", individualmente considerados, menos de diez (10) SMLMV (Ley 1819/2016)
                   if(tipoIdentificacionAportante.intValue() == 2)
                   {
                         
                        try 
                        {

                            //entre mayo 2013  y  diciembre 2016
                            fechaCree1 = formateador.parse("30/04/2013");
                            fechaCree2 = formateador.parse("01/01/2017");

                            if(fechaNomina != null && fechaNomina.after(fechaCree1) && fechaNomina.before(fechaCree2) && valorCalculado.doubleValue() <= smml10.doubleValue())
                            {
                                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_ICBF" + anyoMesDetalleKey(obj), new BigDecimal("0"));
                           //System.out.println("::ANDRES93:: getSujetoPasivoImpuestoCree : " + 0);
                            }

                            //a partir de enero 2017 
                            fechaCree1 = formateador.parse("31/12/2016");


                            if (fechaNomina != null && fechaNomina.after(fechaCree1) && valorCalculado.doubleValue() < smml10.doubleValue())
                            {
                                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_ICBF" + anyoMesDetalleKey(obj), new BigDecimal("0"));
                                //System.out.println("::ANDRES94:: valorCalculado : " + 0);
                            }
                        
                    } catch (ParseException ex) {
                        Logger.getLogger(NominaModoEjecucion.class.getName()).log(Level.SEVERE, null, ex);
                    }        
                               

                       
                   }

               
                   
                   //2- si el campo “SUJETO PASIVO DEL IMPUESTO SOBRE LA RENTA PARA LA EQUIDAD CREE” está marcado con ""SI"", el campo 
                   //TIPO DOCUMENTO APORTANTE"" es diferente a ""NI"",  el periodo fiscalizado es posterior a abril de 2013, y 
                   //existe mas de un trabajador en el mes de la nomina que se esta fiscalizando,  el IBC ICBF será igual a cero (0) para los trabajadores que 
                   //""devenguen"", individualmente considerados, menos de diez (10) salarios mínimos mensuales legales vigentes.
                   if(tipoIdentificacionAportante.intValue() != 2)
                   {

                        try 
                        {
                            //System.out.println("::ANDRES72:: tipoIdentificacionAportante diferente a 2");

                            //BigDecimal cantidadEmp = gestorProgramaDao.obtenerEmpleadoPeriodo(obj.getNominaDetalle());

                            //el periodo fiscalizado es posterior a abril de 2013
                            fechaCree1 = formateador.parse("30/04/2013");
                           

                            if (fechaNomina != null && fechaNomina.after(fechaCree1) && cantidadEmp.compareTo(BigDecimal.ONE) > 1 && valorCalculado.doubleValue() < smml10.doubleValue())
                            {
                                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_ICBF" + anyoMesDetalleKey(obj), new BigDecimal("0"));
                                //System.out.println("::ANDRES95:: tipoIdentificacionAportante : " + 0);
                            }


                        } catch (ParseException ex) {
                            Logger.getLogger(NominaModoEjecucion.class.getName()).log(Level.SEVERE, null, ex);
                        }
                   }
                   
                   
                   
                   
                   
                   
                   
                   
                   
                   
            }
        }

        // Regla #98 <TARIFA ICBF>
        rst = gestorProgramaDao.tarifaICBF(obj.getNomina(), obj.getNominaDetalle());
        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_ICBF" + anyoMesDetalleKey(obj), rst);
        BigDecimal rst05 = new BigDecimal("1");
        rst05 = rst05.divide(new BigDecimal("2"));
        BigDecimal rst15 = new BigDecimal("3");
        rst15 = rst15.divide(new BigDecimal("2"));
        BigDecimal rst075 = rst15.divide(new BigDecimal("2"));
        BigDecimal rst225 = rst075.multiply(new BigDecimal("3"));

        if (null != obj.getNominaDetalle().getCondEspEmp()) {
            switch (obj.getNominaDetalle().getCondEspEmp()) {
                case "LEY 590/2000 AÑO 1":
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_ICBF" + anyoMesDetalleKey(obj), rst075);
                    break;
                case "LEY 590/2000 AÑO 2":
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_ICBF" + anyoMesDetalleKey(obj), rst15);
                    break;
                case "LEY 590/2000 AÑO 3":
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_ICBF" + anyoMesDetalleKey(obj), rst225);
                    break;
                case "LEY 1429 Col AÑO 1,2":
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_ICBF" + anyoMesDetalleKey(obj), new BigDecimal("0"));
                    break;
                case "LEY 1429 Col AÑO 3":
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_ICBF" + anyoMesDetalleKey(obj), rst075);
                    break;
                case "LEY 1429 Col AÑO 4":
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_ICBF" + anyoMesDetalleKey(obj), rst15);
                    break;
                case "LEY 1429 Col AÑO 5":
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_ICBF" + anyoMesDetalleKey(obj), rst225);
                    break;
                case "LEY 1429 AGV AÑO 1-8":
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_ICBF" + anyoMesDetalleKey(obj), new BigDecimal("0"));
                    break;
                case "LEY 1429 AGV AÑO 9":
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_ICBF" + anyoMesDetalleKey(obj), rst15);
                    break;
                case "LEY 1429 AGV AÑO 10":
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_ICBF" + anyoMesDetalleKey(obj), rst225);
                    break;
                case "Soc.declaradas ZF.Art20 Ley 1607":
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_ICBF" + anyoMesDetalleKey(obj), new BigDecimal("3"));
                    break;
                case "Conv.Sub.Fam.Art.17Ley 344/96":
                    infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_ICBF" + anyoMesDetalleKey(obj), new BigDecimal("0"));
                    break;
                default:
                    break;
            }
        }

        // Regla #99 <COTIZACION OBLIGATORIA ICBF>
        rst = mulValorReglas(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#IBC_ICBF" + anyoMesDetalleKey(obj)),
                infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_ICBF" + anyoMesDetalleKey(obj)));
        rst = rst.divide(new BigDecimal("100"));
        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_OBL_ICBF" + anyoMesDetalleKey(obj), roundValor100(rst));

        // Regla #100 <TARIFA PILA ICBF>
        if (pilaDepurada != null) {
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TARIFA_PILA_ICBF" + anyoMesDetalleKey(obj), pilaDepurada.getTarifaAportesIcbf());
        }


        
        
        
        
        
        // Regla #101 <COTIZACION PAGADA PILA ICBF>
        //Se debe colocar valor de la cotización de SENA registrado en el PILA DEPURADA de 
        //acuerdo a la consulta por número DOCUMENTO APORTANTE, 
        //número NUMERO DOCUMENTO ACTUAL DEL COTIZANTE, mes y año de la fiscalización.
        
        if(StringUtils.isNotBlank(obj.getNominaDetalle().getCargueManualPilaIcbf().toString()) && convertValorRegla(obj.getNominaDetalle().getCargueManualPilaIcbf().toString()).intValue() > 0)
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_PAGADA_PILA_ICBF" + anyoMesDetalleKey(obj), obj.getNominaDetalle().getCargueManualPilaIcbf().toString());
        else
        {
            if (pilaDepurada != null && pilaDepurada.getValorAportesParafiscalesIcbf() != null) 
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_PAGADA_PILA_ICBF" + anyoMesDetalleKey(obj),pilaDepurada.getValorAportesParafiscalesIcbf());
            else
            {
                PilaDepurada pilaDepuradaRealizoAportes = gestorProgramaDao.obtegerPilaDepuradaNominaDetalleCotizanteRealizoAportes(obj.getNomina(), obj.getNominaDetalle());

                    //En caso de no encontrar información, se debe colocar valor de la cotización 
                    //de SENA registrado en el PILA DEPURADA de acuerdo a la consulta por número 
                    //DOCUMENTO APORTANTE, número DOCUMENTO CON EL QUE REALIZO APORTES DEL COTIZANTE, mes y año de la fiscalización"
                    if(pilaDepuradaRealizoAportes != null && pilaDepuradaRealizoAportes.getValorAportesParafiscalesIcbf() != null)
                        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_PAGADA_PILA_ICBF" + anyoMesDetalleKey(obj),pilaDepuradaRealizoAportes.getValorAportesParafiscalesIcbf());

            }
        }
        
        

        
        

        // Regla #102 <AJUSTE ICBF>
        rst = minusValorReglas(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_OBL_ICBF" + anyoMesDetalleKey(obj)),
                infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_PAGADA_PILA_ICBF" + anyoMesDetalleKey(obj)));
        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#AJUSTE_ICBF" + anyoMesDetalleKey(obj), rst);

        // Regla #103 <CONCEPTO AJUSTE ICBF>
        rst = convertValorRegla(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#AJUSTE_ICBF" + anyoMesDetalleKey(obj)));

        BigDecimal conceAjuICBF = convertValorRegla(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#COTIZ_PAGADA_PILA_ICBF" + anyoMesDetalleKey(obj)));

        if (conceAjuICBF == null) {
            conceAjuICBF = BigDecimal.ZERO;
        }

        if (rst.intValue() >= 1000) {
            if (conceAjuICBF.compareTo(BigDecimal.ZERO) == 0) {
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CONCEPTO_AJUSTE_ICBF" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.MORA_DESC);
            } else {
                infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CONCEPTO_AJUSTE_ICBF" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.INEXACTO_DESC);
            }
        }

        // Regla #104 <TIPO DE INCUMPLIMIENTO ICBF>
        rst = convertValorRegla(infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#AJUSTE_ICBF" + anyoMesDetalleKey(obj)));
        if (rst.intValue() >= 1000) {
            String conceAjuIcbf = (String) infoNegocio.get(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#CONCEPTO_AJUSTE_ICBF" + anyoMesDetalleKey(obj));
            if (null != conceAjuIcbf) {
                switch (conceAjuIcbf) {
                    case ConstantesGestorPrograma.OMISO_DESC:
                        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TIPO_INCUMPLIMIENTO_ICBF" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.OMISO_NOMBRE);
                        break;
                    case ConstantesGestorPrograma.MORA_DESC:
                        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TIPO_INCUMPLIMIENTO_ICBF" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.MORA_NOMBRE);
                        break;
                    case ConstantesGestorPrograma.INEXACTO_DESC:
                        infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#TIPO_INCUMPLIMIENTO_ICBF" + anyoMesDetalleKey(obj), ConstantesGestorPrograma.INEXACTO_NOMBRE);
                        break;
                    default:
                        break;
                }
            } 
        }
        
        
        //System.out.println("::ANDRES100:: procesarReglasNoFormula pilaDepurada: ");
        

        // Regla #106 <PLANILLA PILA CARGADA>		
        if (pilaDepurada != null) {
            infoNegocio.put(obj.getNominaDetalle().getNumeroIdentificacionActual() + "#PLANILLA_PILA_CARGADA" + anyoMesDetalleKey(obj), pilaDepurada.getPlanilla());
        }
        
        // Finaliza el proceso para una cédula	        

    } // Aquí termina el método de <procesarReglasNoFormula>

    /**
     * Método que carga la lista LST_ADMINISTRADORA_PILA
     */
    void cargarLstAdministradoraPila(GestorProgramaDao gestorProgramaDao) {
        try {
            LST_ADMINISTRADORA_PILA = gestorProgramaDao.getLstAdministradoraPila();
            
            //System.out.println("::ANDRES4:: size LST_ADMINISTRADORA_PILA: " + LST_ADMINISTRADORA_PILA.size());
            
        } catch (Exception e) {
            LOG.error("cargarLstAdministradoraPila - ERROR Exception", e);
        }
    }
    /**
     * Método que borra la lista LST_ADMINISTRADORA_PILA
     */
    @Override
    public void cerrarLstAdministradoraPila() {
        LST_ADMINISTRADORA_PILA = null;
    }


}
