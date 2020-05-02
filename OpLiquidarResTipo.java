
package co.gov.ugpp.parafiscales.servicios.liquidador.srvaplliquidacion;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;
import co.gov.ugpp.parafiscales.servicios.liquidador.contextorespuestatipo.v1.ContextoRespuestaTipo;
import co.gov.ugpp.parafiscales.servicios.liquidador.hojacalculotipo.v1.HojaCalculoTipo;


/**
 * <p>Clase Java para OpLiquidarResTipo complex type.
 * 
 * <p>El siguiente fragmento de esquema especifica el contenido que se espera que haya en esta clase.
 * 
 * <pre>
 * &lt;complexType name="OpLiquidarResTipo">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="contextoRespuesta" type="{http://www.ugpp.gov.co/esb/schema/ContextoRespuestaTipo/v1}ContextoRespuestaTipo"/>
 *         &lt;element name="hojaCalculoLiquidacion" type="{http://www.ugpp.gov.co/schema/Liquidaciones/HojaCalculoTipo/v1}HojaCalculoTipo" minOccurs="0"/>
 *         &lt;element name="esIncumplimiento" type="{http://www.w3.org/2001/XMLSchema}boolean" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "OpLiquidarResTipo", propOrder = {
    "contextoRespuesta",
    "hojaCalculoLiquidacion",
    "esIncumplimiento"
})
public class OpLiquidarResTipo {

    @XmlElement(required = true)
    protected ContextoRespuestaTipo contextoRespuesta;
    protected HojaCalculoTipo hojaCalculoLiquidacion;
    @XmlElement(nillable = true)
    protected Boolean esIncumplimiento;

    /**
     * Obtiene el valor de la propiedad contextoRespuesta.
     * 
     * @return
     *     possible object is
     *     {@link ContextoRespuestaTipo }
     *     
     */
    public ContextoRespuestaTipo getContextoRespuesta() {
        return contextoRespuesta;
    }

    /**
     * Define el valor de la propiedad contextoRespuesta.
     * 
     * @param value
     *     allowed object is
     *     {@link ContextoRespuestaTipo }
     *     
     */
    public void setContextoRespuesta(ContextoRespuestaTipo value) {
        this.contextoRespuesta = value;
    }

    /**
     * Obtiene el valor de la propiedad hojaCalculoLiquidacion.
     * 
     * @return
     *     possible object is
     *     {@link HojaCalculoTipo }
     *     
     */
    public HojaCalculoTipo getHojaCalculoLiquidacion() {
        return hojaCalculoLiquidacion;
    }

    /**
     * Define el valor de la propiedad hojaCalculoLiquidacion.
     * 
     * @param value
     *     allowed object is
     *     {@link HojaCalculoTipo }
     *     
     */
    public void setHojaCalculoLiquidacion(HojaCalculoTipo value) {
        this.hojaCalculoLiquidacion = value;
    }

    /**
     * Obtiene el valor de la propiedad esIncumplimiento.
     * 
     * @return
     *     possible object is
     *     {@link Boolean }
     *     
     */
    public Boolean isEsIncumplimiento() {
        return esIncumplimiento;
    }

    /**
     * Define el valor de la propiedad esIncumplimiento.
     * 
     * @param value
     *     allowed object is
     *     {@link Boolean }
     *     
     */
    public void setEsIncumplimiento(Boolean value) {
        this.esIncumplimiento = value;
    }

}
