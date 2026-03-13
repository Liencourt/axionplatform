from django import forms
from .models import EventoCalendario, Loja


class EventoCalendarioForm(forms.ModelForm):
    class Meta:
        model = EventoCalendario
        fields = ['nome', 'data_inicio', 'data_fim', 'loja']
        widgets = {
            'nome': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Ex: Black Friday, Aniversário'}),
            'data_inicio': forms.DateInput(attrs={'class': 'form-control', 'type': 'date'}),
            'data_fim': forms.DateInput(attrs={'class': 'form-control', 'type': 'date'}),
            'loja': forms.Select(attrs={'class': 'form-select'}),
        }

    def __init__(self, *args, **kwargs):
        # Capturamos a empresa que será passada pela view
        empresa = kwargs.pop('empresa', None)
        super(EventoCalendarioForm, self).__init__(*args, **kwargs)
        
        if empresa:
            # A MÁGICA AQUI: O Dropdown só vai listar as lojas desta empresa específica!
            self.fields['loja'].queryset = Loja.objects.filter(empresa=empresa, ativo=True).order_by('nome')
            self.fields['loja'].empty_label = "Evento Global (Todas as Lojas)"

    def clean(self):
        cleaned_data = super().clean()
        data_inicio = cleaned_data.get("data_inicio")
        data_fim = cleaned_data.get("data_fim")

        # Validação de Segurança: A data final não pode ser antes da inicial
        if data_inicio and data_fim and data_fim < data_inicio:
            raise forms.ValidationError("A data de término não pode ser anterior à data de início.")
        
        return cleaned_data
