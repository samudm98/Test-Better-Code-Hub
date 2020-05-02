
package co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion;

import co.gov.ugpp.parafiscales.servicios.liquidador.conceptocontabletipo.v1.ConceptoContableTipo;
import co.gov.ugpp.parafiscales.servicios.liquidador.contextorespuestatipo.v1.ContextoRespuestaTipo;
import co.gov.ugpp.parafiscales.servicios.liquidador.contextotransaccionaltipo.v1.ContextoTransaccionalTipo;
import co.gov.ugpp.parafiscales.servicios.liquidador.criterioordenamientotipo.v1.CriterioOrdenamientoTipo;
import co.gov.ugpp.parafiscales.servicios.liquidador.fallotipo.v1.FalloTipo;
import co.gov.ugpp.parafiscales.servicios.liquidador.errortipo.v1.ErrorTipo;

import co.gov.ugpp.parafiscales.servicios.liquidador.detallehojacalculotipo.v1.DetalleHojaCalculoTipo;
import co.gov.ugpp.parafiscales.servicios.liquidador.hojacalculotipo.v1.HojaCalculoTipo;

import co.gov.ugpp.parafiscales.servicios.liquidador.expedientetipo.v1.ExpedienteTipo;
import co.gov.ugpp.parafiscales.servicios.liquidador.seriedocumentaltipo.v1.SerieDocumentalTipo;

import co.gov.ugpp.parafiscales.servicios.liquidador.identificaciontipo.v1.IdentificacionTipo;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.annotation.XmlElementDecl;
import javax.xml.bind.annotation.XmlRegistry;
import javax.xml.namespace.QName;


/**
 * This object contains factory methods for each 
 * Java content interface and Java element interface 
 * generated in the co.gov.ugpp.parafiscales.servicios.liquidador.srvAplLiquidador package. 
 * <p>An ObjectFactory allows you to programatically 
 * construct new instances of the Java representation 
 * for XML content. The Java representation of XML 
 * content can consist of schema derived interfaces 
 * and classes representing the binding of schema 
 * type definitions, element declarations and model 
 * groups.  Factory methods for each of these are 
 * provided in this class.
 * 
 */
@XmlRegistry
public class ObjectFactory {

    private final static QName _OpRecibirHojaCalculoLiquidadorSol_QNAME = new QName("http://www.ugpp.gov.co/Transversales/SrvIntProcLiquidador/v1", "OpRecibirHojaCalculoLiquidadorSol");
    private final static QName _OpRecibirHojaCalculoLiquidadorResp_QNAME = new QName("http://www.ugpp.gov.co/Transversales/SrvIntProcLiquidador/v1", "OpRecibirHojaCalculoLiquidadorResp");
    private final static QName _MsjOpRecibirHojaCalculoLiquidadorFallo_QNAME = new QName("http://www.ugpp.gov.co/Transversales/SrvIntProcLiquidador/v1", "msjOpRecibirHojaCalculoLiquidadorFallo");
    private final static QName _OpRecibirHojaCalculoLiquidadorFallo_QNAME = new QName("http://www.ugpp.gov.co/Transversales/SrvIntProcLiquidador/v1", "OpRecibirHojaCalculoLiquidadorFallo");
    
    /**
     * Create a new ObjectFactory that can be used to create new instances of schema derived classes for package: co.gov.ugpp.parafiscales.servicios.liquidador.srvAplLiquidador
     * 
     */
    public ObjectFactory() {
    }

    /**
     * Create an instance of {@link FalloTipo }
     * 
     */
    public FalloTipo createFalloTipo() {
        return new FalloTipo();
    }



    /**
     * Create an instance of {@link ConceptoContableTipo }
     * 
     */
    public ConceptoContableTipo createConceptoContableTipo() {
        return new ConceptoContableTipo();
    }

    /**
     * Create an instance of {@link DetalleHojaCalculoTipo }
     * 
     */
    public DetalleHojaCalculoTipo createDetalleHojaCalculoTipo() {
        return new DetalleHojaCalculoTipo();
    }

    /**
     * Create an instance of {@link HojaCalculoTipo }
     * 
     */
    public HojaCalculoTipo createHojaCalculoTipo() {
        return new HojaCalculoTipo();
    }



    /**
     * Create an instance of {@link IdentificacionTipo }
     * 
     */
    public IdentificacionTipo createIdentificacionTipo() {
        return new IdentificacionTipo();
    }

    /**
     * Create an instance of {@link ExpedienteTipo }
     * 
     */
    public ExpedienteTipo createExpedienteTipo() {
        return new ExpedienteTipo();
    }

    /**
     * Create an instance of {@link SerieDocumentalTipo }
     * 
     */
    public SerieDocumentalTipo createSerieDocumentalTipo() {
        return new SerieDocumentalTipo();
    }

    /**
     * Create an instance of {@link ErrorTipo }
     * 
     */
    public ErrorTipo createErrorTipo() {
        return new ErrorTipo();
    }



    /**
     * Create an instance of {@link ContextoRespuestaTipo }
     * 
     */
    public ContextoRespuestaTipo createContextoRespuestaTipo() {
        return new ContextoRespuestaTipo();
    }

    /**
     * Create an instance of {@link ContextoTransaccionalTipo }
     * 
     */
    public ContextoTransaccionalTipo createContextoTransaccionalTipo() {
        return new ContextoTransaccionalTipo();
    }

    public CriterioOrdenamientoTipo createCriterioOrdenamientoTipo() {
        return new CriterioOrdenamientoTipo();
    }
    
    

    @XmlElementDecl(namespace = "http://www.ugpp.gov.co/Transversales/SrvIntProcLiquidador/v1", name = "OpRecibirHojaCalculoLiquidadorFallo")
    public JAXBElement<FalloTipo> createOpRecibirHojaCalculoLiquidadorFallo(FalloTipo value) {
        return new JAXBElement<FalloTipo>(_OpRecibirHojaCalculoLiquidadorFallo_QNAME, FalloTipo.class, null, value);
    }

   

}
